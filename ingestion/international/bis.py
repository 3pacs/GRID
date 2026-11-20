"""
GRID BIS Statistics API ingestion module.

Pulls credit-to-GDP gap, cross-border banking flows, and property prices
from the Bank for International Settlements SDMX REST API.
"""

from __future__ import annotations

import time
from datetime import date
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from ingestion.base import BasePuller
from ingestion.international import sdmx_json
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

# BIS series: "<dataflow>/<SDMX key>" -> feature name.
#
# 2026-11-04: the old keys ("total_credit/Q/5J/P/N/I/B/770", ...) were never
# valid SDMX data queries -- stats.bis.org answered every one with HTTP 400
# ("Invalid Data Query syntax ... unexpected 5th path parameter"), which
# tenacity retried and wrapped as RetryError[HTTPError], 40 ERROR lines/day.
# These are the real BIS dataflows, each verified by a probe from grid-svr:
#   WS_CREDIT_GAP  FREQ.BORROWERS_CTY.TC_BORROWERS.TC_LENDERS.CG_DTYPE
#                  (P = private non-financial sector, A = all lenders,
#                   C = credit-to-GDP gap, actual minus trend)
#   WS_LBS_D_PUB   locational banking statistics; F = FX- and break-adjusted
#                  change (flow), C = claims, all instruments/currencies,
#                  all reporting countries (5J) vs all counterparties (5J), USD mn
#   WS_SPP         selected residential property prices; XW = world
#                  aggregate, R = real, 628 = index 2010=100
#   WS_TC          total credit; P = private non-financial sector, A = all
#                  lenders, M = market value, 770 = % of GDP, A = adjusted
#                  for breaks. 2026-11-09: moved here from the DBnomics
#                  mirror, which stops at 2024-Q4 (143.0) while BIS itself
#                  publishes to 2026-Q1 (139.7); us_credit_gdp_bis had never
#                  stored a row, so no source mixing.
BIS_SERIES_LIST: dict[str, str] = {
    "WS_TC/Q.US.P.A.M.770.A": "us_credit_gdp_bis",
    "WS_CREDIT_GAP/Q.US.P.A.C": "bis_credit_gdp_gap_us",
    "WS_CREDIT_GAP/Q.CN.P.A.C": "bis_credit_gdp_gap_cn",
    "WS_CREDIT_GAP/Q.DE.P.A.C": "bis_credit_gdp_gap_de",
    "WS_LBS_D_PUB/Q.F.C.A.TO1.A.5J.A.5A.A.5J.N": "bis_global_cbflow_usd",
    "WS_SPP/Q.XW.R.628": "bis_property_prices_g20",
}

_BIS_BASE_URL = sdmx_json.BIS_DATA_URL
_RATE_LIMIT_DELAY: float = 2.0


def _is_retryable(exc: BaseException) -> bool:
    """Retry transport failures and 5xx/429 only; a 4xx query error is final."""
    if isinstance(exc, requests.HTTPError):
        status = getattr(exc.response, "status_code", None)
        return status is None or status >= 500 or status == 429
    return isinstance(exc, (requests.ConnectionError, requests.Timeout))


def _split_ref(dataset_ref: str) -> tuple[str, str]:
    flow, _, key = dataset_ref.partition("/")
    if not flow or not key:
        raise ValueError(f"BIS dataset_ref must be '<flow>/<key>', got {dataset_ref!r}")
    return flow, key


class BISPuller(BasePuller):
    """Pulls statistics from the Bank for International Settlements API."""

    SOURCE_NAME = "BIS"
    SOURCE_CONFIG = {"base_url": "https://stats.bis.org/api/v2", "cost_tier": "FREE", "latency_class": "MONTHLY", "pit_available": True, "revision_behavior": "RARE", "trust_score": "HIGH", "priority_rank": 12}

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info("BISPuller initialised — source_id={sid}", sid=self.source_id)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=10),
        retry=retry_if_exception(_is_retryable),
        reraise=True,
    )
    def _fetch_observations(self, dataset_ref: str, start_period: str | None) -> list[tuple[date, float]]:
        flow, key = _split_ref(dataset_ref)
        return sdmx_json.fetch_bis_series(flow, key, start_period)

    @staticmethod
    def _parse_period(period_str: str) -> date | None:
        return sdmx_json.parse_period(period_str)

    def pull_series(
        self,
        dataset_ref: str,
        start_date: str | date = "1970-01-01",
        end_date: str | date | None = None,
    ) -> dict[str, Any]:
        """Fetch a single BIS series and insert into raw_series."""
        feature_name = BIS_SERIES_LIST.get(dataset_ref, dataset_ref)
        log.info("Pulling BIS series {dr} ({fn})", dr=dataset_ref, fn=feature_name)

        result: dict[str, Any] = {
            "series_id": feature_name,
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        try:
            observations = self._fetch_observations(dataset_ref, sdmx_json.lookback_start_year(start_date))

            if not observations:
                result["status"] = "PARTIAL"
                result["errors"].append("No data returned")
                return result

            inserted = 0
            with self.engine.begin() as conn:
                # Insert a vintage only when the value is new or revised. The
                # 1-hour _row_exists window re-inserted the whole 2-year
                # lookback on every run (bis_credit_gdp_gap_us: 108 rows for
                # 9 dates on 2026-11-09).
                stored = self._latest_stored_values(
                    feature_name, conn, start_date=min(d for d, _ in observations),
                )
                for obs_date_val, value in observations:
                    if stored.get(obs_date_val) == value:
                        continue
                    stored[obs_date_val] = value
                    conn.execute(
                        text(
                            "INSERT INTO raw_series "
                            "(series_id, source_id, obs_date, value, pull_status) "
                            "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                        ),
                        {"sid": feature_name, "src": self.source_id, "od": obs_date_val, "val": value},
                    )
                    inserted += 1

            result["rows_inserted"] = inserted
            log.info("BIS {fn}: inserted {n} rows", fn=feature_name, n=inserted)

        except Exception as exc:
            # Upstream/network failures are operational, not code bugs.
            level = log.error if isinstance(exc, (KeyError, AttributeError, TypeError)) else log.warning
            level("BIS pull failed for {dr}: {err}", dr=dataset_ref, err=str(exc)[:300])
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        time.sleep(_RATE_LIMIT_DELAY)
        return result

    def pull_all(self, start_date: str | date = "1970-01-01") -> dict[str, Any]:
        """Pull all BIS series."""
        log.info("Starting BIS bulk pull from {sd}", sd=start_date)
        results = [self.pull_series(dr, start_date) for dr in BIS_SERIES_LIST]

        total_rows = sum(r["rows_inserted"] for r in results)
        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        log.info(
            "BIS bulk pull complete — {ok}/{total} succeeded, {rows} rows",
            ok=succeeded, total=len(results), rows=total_rows,
        )
        return {
            "source": "BIS",
            "total_rows": total_rows,
            "succeeded": succeeded,
            "total": len(results),
            "results": results,
        }
