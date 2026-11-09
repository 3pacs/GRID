"""
GRID Reserve Bank of India (RBI) ingestion module.

Pulls Indian monetary and economic data: policy repo rate, industrial
production growth, and FX reserves.

2026-11-04 rebuild. The old module fetched one generic URL
(``dbie.rbi.org.in/DBIE/dbie.rbi?site=data&actionVal=CSVDATA``) for every
series and expected a JSON list of ``{"date", "value"}`` records -- it never
returned data. Since 2026-08 that host also serves a certificate issued only
for ``data.rbi.org.in`` (SAN: ``DNS:data.rbi.org.in``), so every request
failed TLS hostname verification (``SSLError ... Hostname mismatch``), 24
ERROR lines/day. It is not a missing intermediate: the chain verifies; the
name does not match. The replacement RBI portal (data.rbi.org.in) has no
documented public API, so the series now come from the official
re-publications of RBI data by the BIS and the IMF, which carry stable SDMX
keys (each verified by a probe from grid-svr on 2026-11-04):

* ``india_repo_rate``   BIS WS_CBPOL ``M.IN`` -- central bank policy rate
  (RBI repo rate), monthly, percent.
* ``india_iip_yoy``     IMF PI ``IND.IND.YOY_PCH_PT.Q`` -- industrial
  production, % change year over year. Quarterly: the IMF's monthly India
  IIP series stops at 2017-11, the quarterly one is current.
* ``india_fx_reserves`` IMF IL ``IND.TRGMV_REVS.USD.M`` -- total reserves
  including gold at market value, USD (not millions), monthly.

TLS verification stays on everywhere.
"""

from __future__ import annotations

import time
from datetime import date
from typing import Any, Callable

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from ingestion.base import BasePuller
from ingestion.international import sdmx_json
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

# RBI series definitions: key -> upstream provider, SDMX flow/key, feature.
RBI_SERIES: dict[str, dict[str, str]] = {
    "repo_rate": {
        "provider": "bis",
        "flow": "WS_CBPOL",
        "key": "M.IN",
        "feature": "india_repo_rate",
    },
    "iip_yoy": {
        "provider": "imf",
        "flow": "PI",
        "key": "IND.IND.YOY_PCH_PT.Q",
        "feature": "india_iip_yoy",
    },
    "fx_reserves": {
        "provider": "imf",
        "flow": "IL",
        "key": "IND.TRGMV_REVS.USD.M",
        "feature": "india_fx_reserves",
    },
}

_RATE_LIMIT_DELAY: float = 2.0

_FETCHERS: dict[str, Callable[[str, str, str | None], list[tuple[date, float]]]] = {
    "bis": sdmx_json.fetch_bis_series,
    "imf": sdmx_json.fetch_imf_series,
}


def _is_retryable(exc: BaseException) -> bool:
    """Retry transport failures and 5xx/429 only; a 4xx is final."""
    if isinstance(exc, requests.HTTPError):
        status = getattr(exc.response, "status_code", None)
        return status is None or status >= 500 or status == 429
    return isinstance(exc, (requests.ConnectionError, requests.Timeout))


class RBIPuller(BasePuller):
    """Pulls Indian economic data (RBI series via BIS / IMF SDMX)."""

    SOURCE_NAME = "RBI"
    SOURCE_CONFIG = {"base_url": "https://stats.bis.org/api/v2", "cost_tier": "FREE", "latency_class": "MONTHLY", "pit_available": False, "revision_behavior": "RARE", "trust_score": "HIGH", "priority_rank": 31}

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info("RBIPuller initialised — source_id={sid}", sid=self.source_id)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=10),
        retry=retry_if_exception(_is_retryable),
        reraise=True,
    )
    def _fetch_series(self, config: dict[str, str], start_period: str | None) -> list[tuple[date, float]]:
        fetcher = _FETCHERS[config["provider"]]
        return fetcher(config["flow"], config["key"], start_period)

    def _insert_observations(self, feature_name: str, observations: list[tuple[date, float]]) -> int:
        """Insert a list of (obs_date, value) tuples into raw_series."""
        inserted = 0
        if not observations:
            return 0
        with self.engine.begin() as conn:
            # New or revised values only; the 1-hour _row_exists window
            # re-inserted the 2-year lookback every run (india_iip_yoy:
            # 2,626 rows for 218 dates on 2026-11-09).
            stored = self._latest_stored_values(
                feature_name, conn, start_date=min(d for d, _ in observations),
            )
            for obs_dt, value in observations:
                if stored.get(obs_dt) == value:
                    continue
                stored[obs_dt] = value
                conn.execute(
                    text(
                        "INSERT INTO raw_series "
                        "(series_id, source_id, obs_date, value, pull_status) "
                        "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                    ),
                    {"sid": feature_name, "src": self.source_id, "od": obs_dt, "val": value},
                )
                inserted += 1
        return inserted

    def pull_all(self, start_date: str | date = "2000-01-01") -> dict[str, Any]:
        """Pull all RBI series (BIS/IMF SDMX re-publications of RBI data)."""
        log.info("Starting RBI pull from {sd}", sd=start_date)
        results: list[dict[str, Any]] = []

        for key, config in RBI_SERIES.items():
            feature_name = config["feature"]
            result: dict[str, Any] = {
                "series_id": feature_name,
                "rows_inserted": 0,
                "status": "SUCCESS",
                "errors": [],
            }

            try:
                observations = self._fetch_series(config, sdmx_json.lookback_start_year(start_date))
                if not observations:
                    result["status"] = "PARTIAL"
                    result["errors"].append(
                        f"{config['provider'].upper()} {config['flow']}/{config['key']} returned no observations"
                    )
                else:
                    result["rows_inserted"] = self._insert_observations(feature_name, observations)
                    result["latest_obs_date"] = observations[-1][0].isoformat()

            except Exception as exc:
                level = log.error if isinstance(exc, (KeyError, AttributeError, TypeError)) else log.warning
                level("RBI pull failed for {key}: {err}", key=key, err=str(exc)[:300])
                result["status"] = "FAILED"
                result["errors"].append(str(exc))

            results.append(result)
            time.sleep(_RATE_LIMIT_DELAY)

        total_rows = sum(r["rows_inserted"] for r in results)
        log.info("RBI pull complete — {rows} rows", rows=total_rows)
        return {
            "source": "RBI",
            "total_rows": total_rows,
            "succeeded": sum(1 for r in results if r["status"] == "SUCCESS"),
            "total": len(results),
        }
