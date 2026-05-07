"""
GRID Tiingo data ingestion module — fallback for yfinance.

Uses the Tiingo REST API for reliable OHLCV price data.
Free tier: 50 symbols/hour, 500 requests/day, 30+ years history.

Requires TIINGO_API_KEY in environment (free at https://www.tiingo.com).
"""

from __future__ import annotations

import os
import time
from datetime import date
from typing import Any

import pandas as pd
import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError

from ingestion.base import BasePuller

# Process-scoped circuit breaker for catastrophic DB index corruption.
# Once a single ticker raises psycopg2.errors.IndexCorrupted on
# uq_raw_series_composite, every subsequent ticker in the cycle hits the
# same wall — burning ~30 ERROR rows per cycle until an operator runs
# REINDEX. Flip this once and skip the rest of the cycle so the log
# stays focused on the one actionable line.
_INDEX_CORRUPTED_BREAKER: bool = False

_TIINGO_API_KEY = os.getenv("TIINGO_API_KEY", "")
_BASE_URL = "https://api.tiingo.com"
_RATE_LIMIT_DELAY = 0.2  # seconds between calls (Pro tier — generous limits)
_REQUEST_TIMEOUT = 30

# Map Tiingo fields → GRID series suffix (same as yfinance convention)
_FIELD_MAP: dict[str, str] = {
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "volume": "volume",
    "adjClose": "adj_close",
}


def _tiingo_headers() -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        "Authorization": f"Token {_TIINGO_API_KEY}",
    }


class TiingoPuller(BasePuller):
    """Pulls OHLCV data from Tiingo as a fallback for yfinance."""

    SOURCE_NAME: str = "TIINGO"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://api.tiingo.com",
        "cost_tier": "PAID",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "RARE",
        "trust_score": "HIGH",
        "priority_rank": 6,
    }

    def __init__(self, db_engine: Engine) -> None:
        if not _TIINGO_API_KEY:
            raise ValueError("TIINGO_API_KEY not set — get a free key at https://www.tiingo.com")
        super().__init__(db_engine)
        log.info("TiingoPuller initialised — source_id={sid}", sid=self.source_id)

    def pull_ticker(
        self,
        ticker: str,
        start_date: str | date = "2020-01-01",
        end_date: str | date | None = None,
    ) -> dict[str, Any]:
        """Download OHLCV for a single ticker from Tiingo."""
        result: dict[str, Any] = {
            "ticker": ticker,
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        # Clean ticker: Tiingo uses plain symbols, no ^ prefix
        clean_ticker = ticker.replace("^", "").replace("=F", "").replace("=X", "")

        if end_date is None:
            end_date = date.today()

        url = f"{_BASE_URL}/tiingo/daily/{clean_ticker}/prices"
        params = {
            "startDate": str(start_date),
            "endDate": str(end_date),
            "format": "json",
        }

        try:
            resp = requests.get(
                url, headers=_tiingo_headers(), params=params, timeout=_REQUEST_TIMEOUT
            )

            if resp.status_code == 404:
                log.debug("Tiingo: ticker {t} not found", t=ticker)
                result["status"] = "PARTIAL"
                result["errors"].append(f"Ticker {ticker} not found on Tiingo")
                return result

            resp.raise_for_status()
            data = resp.json()

            if not data:
                result["status"] = "PARTIAL"
                result["errors"].append("No data returned")
                return result

            df = pd.DataFrame(data)
            df["date"] = pd.to_datetime(df["date"]).dt.date
            inserted = 0

            rows_batch: list[dict] = []
            for tiingo_field, grid_field in _FIELD_MAP.items():
                if tiingo_field not in df.columns:
                    continue

                series_id = f"YF:{ticker}:{grid_field}"  # Same naming as yfinance
                col_data = df[["date", tiingo_field]].dropna()

                for _, row in col_data.iterrows():
                    rows_batch.append({
                        "sid": series_id,
                        "src": self.source_id,
                        "od": row["date"],
                        "val": float(row[tiingo_field]),
                    })

            if rows_batch:
                with self.engine.begin() as conn:
                    res = conn.execute(
                        text(
                            "INSERT INTO raw_series "
                            "(series_id, source_id, obs_date, value, pull_status) "
                            "SELECT :sid, :src, :od, :val, 'SUCCESS' "
                            "WHERE NOT EXISTS ("
                            "  SELECT 1 FROM raw_series "
                            "  WHERE series_id = :sid "
                            "    AND source_id = :src "
                            "    AND obs_date = :od "
                            "    AND pull_status = 'SUCCESS'"
                            ")"
                        ),
                        rows_batch,
                    )
                inserted = max(0, int(res.rowcount or 0))

            result["rows_inserted"] = inserted
            log.info("Tiingo {t}: inserted {n} rows", t=ticker, n=inserted)

        except requests.exceptions.HTTPError as e:
            log.warning("Tiingo HTTP error for {t}: {e}", t=ticker, e=str(e))
            result["status"] = "FAILED"
            result["errors"].append(str(e))
        except Exception as exc:
            # Detect IndexCorrupted on uq_raw_series_composite: this is a
            # catastrophic Postgres-level failure that requires an operator
            # to run REINDEX. Every subsequent ticker would raise the same
            # error, so flip a process-scoped breaker and surface ONE
            # actionable line instead of N×30 noisy ERROR rows per cycle.
            global _INDEX_CORRUPTED_BREAKER
            err_str = str(exc)
            if "IndexCorrupted" in err_str or "uq_raw_series_composite" in err_str:
                if not _INDEX_CORRUPTED_BREAKER:
                    _INDEX_CORRUPTED_BREAKER = True
                    log.error(
                        "Tiingo: Postgres index uq_raw_series_composite is "
                        "CORRUPTED — operator must run "
                        "`REINDEX INDEX CONCURRENTLY uq_raw_series_composite;` "
                        "(see scripts/migrations/reindex_raw_series.sql). "
                        "Skipping the rest of this Tiingo cycle to avoid "
                        "log flood. Triggered by ticker {t}: {err}",
                        t=ticker, err=err_str[:200],
                    )
                else:
                    log.debug(
                        "Tiingo: skipping {t} — index corruption breaker tripped",
                        t=ticker,
                    )
                result["status"] = "FAILED"
                result["errors"].append("index corruption — see prior ERROR")
                return result
            log.error("Tiingo pull failed for {t}: {err}", t=ticker, err=err_str)
            result["status"] = "FAILED"
            result["errors"].append(err_str)

        return result

    def pull_all(
        self,
        ticker_list: list[str] | None = None,
        start_date: str | date = "2020-01-01",
    ) -> list[dict[str, Any]]:
        """Pull multiple tickers with rate limiting.

        Merges YF_TICKER_LIST with all tickers from the sector map
        so that every actor with a ticker gets daily price data.
        """
        from ingestion.yfinance_pull import YF_TICKER_LIST

        if ticker_list is not None:
            tickers = ticker_list
        else:
            # Merge base list with sector map tickers for full coverage
            all_tickers = set(YF_TICKER_LIST)
            try:
                from analysis.sector_map import SECTOR_MAP
                for sector in SECTOR_MAP.values():
                    etf = sector.get("etf")
                    if etf:
                        all_tickers.add(etf)
                    for sub in sector.get("subsectors", {}).values():
                        for actor in sub.get("actors", []):
                            t = actor.get("ticker")
                            if t:
                                all_tickers.add(t)
            except Exception:
                pass  # Graceful degradation if sector_map has an issue
            tickers = sorted(all_tickers)
        results = []
        succeeded = 0

        for ticker in tickers:
            res = self.pull_ticker(ticker, start_date=start_date)
            results.append(res)
            if res["status"] == "SUCCESS":
                succeeded += 1
            # Short-circuit the rest of the cycle when the catastrophic
            # IndexCorrupted breaker has tripped — every remaining ticker
            # would re-raise the same error.
            if _INDEX_CORRUPTED_BREAKER:
                log.warning(
                    "Tiingo bulk pull aborted at ticker {t} — "
                    "index corruption breaker tripped (operator must REINDEX)",
                    t=ticker,
                )
                break
            time.sleep(_RATE_LIMIT_DELAY)

        log.info(
            "Tiingo bulk pull complete — {s}/{t} succeeded",
            s=succeeded, t=len(tickers),
        )
        return results
