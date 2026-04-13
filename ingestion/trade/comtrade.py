"""
GRID UN Comtrade v2 bilateral trade flow ingestion module.

Pulls international trade data from the UN Comtrade API v2. Covers bilateral
flows by HS code, with YoY change derivation. Free tier: 500 calls/day.
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from ingestion.base import BasePuller
from tenacity import retry, stop_after_attempt, wait_exponential

# Comtrade query definitions
COMTRADE_QUERIES: list[dict[str, str]] = [
    {"reporterCode": "842", "partnerCode": "0", "cmdCode": "TOTAL", "label": "us_exports_total"},
    {"reporterCode": "842", "partnerCode": "156", "cmdCode": "TOTAL", "label": "us_china_bilateral"},
    {"reporterCode": "0", "partnerCode": "0", "cmdCode": "8542", "label": "global_semi_trade"},
    {"reporterCode": "0", "partnerCode": "0", "cmdCode": "2601", "label": "global_iron_trade"},
    {"reporterCode": "0", "partnerCode": "0", "cmdCode": "2709", "label": "global_crude_trade"},
    {"reporterCode": "0", "partnerCode": "0", "cmdCode": "1001", "label": "global_wheat_trade"},
]

_COMTRADE_BASE_URL = "https://comtradeapi.un.org/public/v1/preview/C/A/HS"
_RATE_LIMIT_DELAY: float = 2.0


class ComtradePuller(BasePuller):
    """Pulls bilateral trade data from UN Comtrade v2 API."""

    SOURCE_NAME = "Comtrade"
    SOURCE_CONFIG = {"base_url": "https://comtradeapi.un.org", "cost_tier": "FREE", "latency_class": "MONTHLY", "pit_available": True, "revision_behavior": "RARE", "trust_score": "HIGH", "priority_rank": 30}

    def __init__(self, db_engine: Engine, api_key: str | None = None) -> None:
        self.api_key = api_key
        super().__init__(db_engine)
        log.info("ComtradePuller initialised — source_id={sid}", sid=self.source_id)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=10))
    def _fetch_comtrade(self, reporter: str, partner: str, cmd_code: str, period: str) -> list[dict]:
        """Fetch trade data from Comtrade API."""
        params: dict[str, str] = {
            "reporterCode": reporter,
            "partnerCode": partner,
            "cmdCode": cmd_code,
            "period": period,
            "flowCode": "M,X",
        }
        if self.api_key:
            params["subscription-key"] = self.api_key

        resp = requests.get(_COMTRADE_BASE_URL, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", [])

    def pull_query(
        self,
        reporter: str,
        partner: str,
        cmd_code: str,
        period_start: int = 1990,
        period_end: int | None = None,
    ) -> dict[str, Any]:
        """Pull a single Comtrade query and compute YoY change."""
        if period_end is None:
            period_end = date.today().year

        series_id = f"COMTRADE:{reporter}:{partner}:{cmd_code}"
        label = None
        for q in COMTRADE_QUERIES:
            if q["reporterCode"] == reporter and q["partnerCode"] == partner and q["cmdCode"] == cmd_code:
                label = q["label"]
                break
        if label is None:
            label = series_id

        log.info("Pulling Comtrade {lbl}", lbl=label)
        result: dict[str, Any] = {
            "series_id": label,
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        try:
            # Pull year by year to stay within free tier limits
            all_values: dict[int, float] = {}
            for year in range(period_start, period_end + 1):
                try:
                    records = self._fetch_comtrade(reporter, partner, cmd_code, str(year))
                    for rec in records:
                        trade_value = rec.get("primaryValue") or rec.get("tradeValue") or 0
                        period = rec.get("period", year)
                        all_values[int(period)] = float(trade_value)
                except Exception as year_exc:
                    log.debug("Comtrade year {y} failed: {err}", y=year, err=str(year_exc))

                time.sleep(_RATE_LIMIT_DELAY)

            # Insert raw values
            inserted = 0
            with self.engine.begin() as conn:
                for year, value in sorted(all_values.items()):
                    obs_dt = date(year, 1, 1)
                    if self._row_exists(label, obs_dt, conn):
                        continue
                    conn.execute(
                        text(
                            "INSERT INTO raw_series "
                            "(series_id, source_id, obs_date, value, pull_status) "
                            "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                        ),
                        {"sid": label, "src": self.source_id, "od": obs_dt, "val": value},
                    )
                    inserted += 1

                # Compute YoY change
                sorted_years = sorted(all_values.keys())
                for i in range(1, len(sorted_years)):
                    prev_val = all_values[sorted_years[i - 1]]
                    curr_val = all_values[sorted_years[i]]
                    if prev_val != 0:
                        yoy = (curr_val - prev_val) / prev_val * 100
                        yoy_id = f"{label}_yoy"
                        obs_dt = date(sorted_years[i], 1, 1)
                        if not self._row_exists(yoy_id, obs_dt, conn):
                            conn.execute(
                                text(
                                    "INSERT INTO raw_series "
                                    "(series_id, source_id, obs_date, value, pull_status) "
                                    "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                                ),
                                {"sid": yoy_id, "src": self.source_id, "od": obs_dt, "val": yoy},
                            )
                            inserted += 1

            result["rows_inserted"] = inserted
            log.info("Comtrade {lbl}: inserted {n} rows", lbl=label, n=inserted)

        except Exception as exc:
            log.error("Comtrade pull failed for {lbl}: {err}", lbl=label, err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        return result

    def pull_all(self, start_year: int = 1990) -> dict[str, Any]:
        """Pull all Comtrade queries."""
        log.info("Starting Comtrade bulk pull from {sy}", sy=start_year)
        results: list[dict[str, Any]] = []

        for query in COMTRADE_QUERIES:
            res = self.pull_query(
                query["reporterCode"],
                query["partnerCode"],
                query["cmdCode"],
                period_start=start_year,
            )
            results.append(res)

        total_rows = sum(r["rows_inserted"] for r in results)
        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        log.info(
            "Comtrade bulk pull complete — {ok}/{total} succeeded, {rows} rows",
            ok=succeeded, total=len(results), rows=total_rows,
        )
        return {
            "source": "Comtrade",
            "total_rows": total_rows,
            "succeeded": succeeded,
            "total": len(results),
        }
