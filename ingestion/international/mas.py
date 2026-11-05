"""
GRID Monetary Authority of Singapore (MAS) ingestion module.

2026-11-04 rebuild. The legacy MAS datastore API
(``eservices.mas.gov.sg/api/action/datastore/search.json``) is retired: it
answers every resource id with an HTML maintenance page, so each cycle failed
with ``RetryError[JSONDecodeError]`` after three retries. MAS now serves its
statistics only through the APIMG gateway, which requires a registered API
key (``eservices.mas.gov.sg/apimg-gw/...`` answers 401 anonymously).

What is still reachable without a key (verified by probe from grid-svr):

* ``singapore_fx_reserves`` -- IMF International Liquidity dataflow,
  ``IL/SGP.TRGMV_REVS.USD.M`` (total reserves incl. gold at market value,
  USD, monthly). Singapore reports its reserves to the IMF; this is the
  same official number MAS publishes, with a ~2 month lag.

Not available (reported once per process, never attempted every cycle):

* ``singapore_sora`` and ``singapore_m2`` -- MAS-only series; need an APIMG
  key. Singapore does not report to the IMF monetary-aggregate or
  interest-rate dataflows (MFS_MA / MFS_IR return no SGP series).
* ``singapore_sibor_3m`` -- SIBOR was discontinued on 2024-12-31 (replaced
  by SORA); there is nothing left to pull.
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

# feature -> (IMF flow, key) for the series still available keylessly.
MAS_SERIES: dict[str, tuple[str, str]] = {
    "singapore_fx_reserves": ("IL", "SGP.TRGMV_REVS.USD.M"),
}

# feature -> why it is no longer pulled (logged once per process).
MAS_UNAVAILABLE_SERIES: dict[str, str] = {
    "singapore_sora": "MAS-only series; the legacy datastore API is retired and the MAS APIMG gateway requires an API key",
    "singapore_m2": "MAS-only series; the legacy datastore API is retired and the MAS APIMG gateway requires an API key",
    "singapore_sibor_3m": "SIBOR was discontinued on 2024-12-31",
}

_RATE_LIMIT_DELAY: float = 1.0
_UNAVAILABLE_LOGGED: bool = False


def _is_retryable(exc: BaseException) -> bool:
    """Retry transport failures and 5xx/429 only; a 4xx is final."""
    if isinstance(exc, requests.HTTPError):
        status = getattr(exc.response, "status_code", None)
        return status is None or status >= 500 or status == 429
    return isinstance(exc, (requests.ConnectionError, requests.Timeout))


class MASPuller(BasePuller):
    """Pulls Singapore monetary data (MAS series reported to the IMF)."""

    SOURCE_NAME = "MAS_SG"
    SOURCE_CONFIG = {"base_url": "https://api.imf.org/external/sdmx/3.0", "cost_tier": "FREE", "latency_class": "MONTHLY", "pit_available": True, "revision_behavior": "RARE", "trust_score": "HIGH", "priority_rank": 29}

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info("MASPuller initialised — source_id={sid}", sid=self.source_id)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=10),
        retry=retry_if_exception(_is_retryable),
        reraise=True,
    )
    def _fetch(self, flow: str, key: str, start_period: str | None) -> list[tuple[date, float]]:
        return sdmx_json.fetch_imf_series(flow, key, start_period)

    def pull_series(self, feature_name: str, start_date: str | date = "2000-01-01") -> dict[str, Any]:
        """Pull one Singapore series and insert into raw_series."""
        flow, key = MAS_SERIES[feature_name]
        result: dict[str, Any] = {
            "series_id": feature_name,
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }
        try:
            observations = self._fetch(flow, key, sdmx_json.lookback_start_year(start_date))
            if not observations:
                result["status"] = "PARTIAL"
                result["errors"].append(f"IMF {flow}/{key} returned no observations")
                return result
            inserted = 0
            with self.engine.begin() as conn:
                for obs_dt, value in observations:
                    if self._row_exists(feature_name, obs_dt, conn):
                        continue
                    conn.execute(
                        text(
                            "INSERT INTO raw_series "
                            "(series_id, source_id, obs_date, value, pull_status) "
                            "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                        ),
                        {"sid": feature_name, "src": self.source_id, "od": obs_dt, "val": value},
                    )
                    inserted += 1
            result["rows_inserted"] = inserted
            log.info("MAS {fn}: inserted {n} rows", fn=feature_name, n=inserted)
        except Exception as exc:
            level = log.error if isinstance(exc, (KeyError, AttributeError, TypeError)) else log.warning
            level("MAS pull failed for {fn} ({fl}/{k}): {err}", fn=feature_name, fl=flow, k=key, err=str(exc)[:200])
            result["status"] = "FAILED"
            result["errors"].append(str(exc))
        time.sleep(_RATE_LIMIT_DELAY)
        return result

    def pull_all(self, start_date: str | date = "2000-01-01") -> dict[str, Any]:
        """Pull every Singapore series that is still reachable."""
        global _UNAVAILABLE_LOGGED
        if not _UNAVAILABLE_LOGGED:
            _UNAVAILABLE_LOGGED = True
            log.info(
                "MAS: not pulling {n} series with no keyless source: {s}",
                n=len(MAS_UNAVAILABLE_SERIES),
                s="; ".join(f"{k} ({v})" for k, v in MAS_UNAVAILABLE_SERIES.items()),
            )
        results = [self.pull_series(fn, start_date) for fn in MAS_SERIES]
        total_rows = sum(r["rows_inserted"] for r in results)
        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        log.info(
            "MAS bulk pull complete — {ok}/{total} succeeded, {rows} rows",
            ok=succeeded, total=len(results), rows=total_rows,
        )
        return {
            "source": "MAS_SG",
            "total_rows": total_rows,
            "succeeded": succeeded,
            "total": len(results),
            "unavailable_series": sorted(MAS_UNAVAILABLE_SERIES),
        }
