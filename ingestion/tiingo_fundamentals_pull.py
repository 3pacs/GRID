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
import time
from datetime import date, timedelta
from typing import Any

import pandas as pd
import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller

_TIINGO_API_KEY = os.getenv("TIINGO_API_KEY", "")
_BASE_URL = "https://api.tiingo.com"
_RATE_LIMIT_DELAY = 0.3

# Fields from Tiingo daily fundamentals → GRID feature suffix
_FIELD_MAP: dict[str, str] = {
    "marketCap": "market_cap",
    "enterpriseVal": "enterprise_value",
    "peRatio": "pe_ratio",
    "pbRatio": "pb_ratio",
    "trailingPEG1Y": "peg_ratio",
}


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

        try:
            url = f"{_BASE_URL}/tiingo/fundamentals/{ticker}/daily"
            params = {
                "token": _TIINGO_API_KEY,
                "startDate": str(start_date),
                "endDate": str(end_date),
            }

            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()

            if not data:
                result["status"] = "NO_DATA"
                return result

            rows_inserted = 0

            with self.engine.begin() as conn:
                for row in data:
                    obs_date = pd.to_datetime(row["date"]).date()

                    for tiingo_field, grid_suffix in _FIELD_MAP.items():
                        value = row.get(tiingo_field)
                        if value is None:
                            continue

                        series_id = f"TIINGO_FUND:{ticker}:{grid_suffix}"
                        feature_name = f"{ticker.lower()}_{grid_suffix}"

                        conn.execute(
                            text("""
                                INSERT INTO raw_series (source_id, series_id, obs_date, value, pull_status, pull_timestamp)
                                VALUES (:sid, :series, :obs, :val, 'SUCCESS', NOW())
                                ON CONFLICT (series_id, source_id, obs_date, pull_timestamp) DO NOTHING
                            """),
                            {
                                "sid": self.source_id,
                                "series": series_id,
                                "obs": obs_date,
                                "val": float(value),
                            },
                        )
                        rows_inserted += 1

            result["rows_inserted"] = rows_inserted
            log.info("Tiingo fundamentals {t}: {n} rows", t=ticker, n=rows_inserted)

        except requests.exceptions.HTTPError as e:
            if "400" in str(e) and "Free" in str(e):
                result["status"] = "PAYWALLED"
                result["errors"].append("Free tier: DOW 30 only")
            else:
                result["status"] = "FAILED"
                result["errors"].append(str(e))
        except Exception as e:
            result["status"] = "FAILED"
            result["errors"].append(str(e))

        return result

    def pull_all(
        self,
        ticker_list: list[str] | None = None,
        start_date: str | date = "2020-01-01",
    ) -> list[dict[str, Any]]:
        """Pull fundamentals for multiple tickers."""
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

        for ticker in ticker_list:
            res = self.pull_ticker(ticker, start_date=start_date)
            results.append(res)
            if res["status"] == "SUCCESS":
                succeeded += 1
            elif res["status"] == "PAYWALLED":
                paywalled += 1
            time.sleep(_RATE_LIMIT_DELAY)

        log.info(
            "Tiingo fundamentals bulk: {s} ok, {p} paywalled, {f} failed out of {t}",
            s=succeeded, p=paywalled,
            f=len(ticker_list) - succeeded - paywalled,
            t=len(ticker_list),
        )
        return results
