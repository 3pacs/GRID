"""
GRID EIA energy data ingestion module.

Pulls Brent/WTI crude oil daily spot prices from the EIA API v2.

Data source: https://api.eia.gov/v2/petroleum/pri/spt/data/
API key required (free): set EIA_API_KEY env var.

Series stored:
- eia.brent_spot: Brent crude oil spot price (USD/bbl)
- eia.wti_spot: WTI crude oil spot price (USD/bbl)
"""

from __future__ import annotations

import os
import time
from datetime import date, timedelta
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

_EIA_BASE = "https://api.eia.gov/v2/petroleum/pri/spt/data/"
_REQUEST_TIMEOUT: int = 30
_SERIES_MAP: dict[str, str] = {"RBRTE": "brent_spot", "RWTC": "wti_spot"}
_SERIES_PREFIX = "eia"


class EIAPuller(BasePuller):
    """Pulls daily petroleum spot prices from the EIA API v2."""

    SOURCE_NAME: str = "EIA"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": _EIA_BASE,
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 20,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        self._api_key = os.environ.get("EIA_API_KEY", "")
        if not self._api_key:
            log.warning("EIA_API_KEY not set -- EIA pulls will fail")

    @retry_on_failure(
        max_attempts=3, backoff=2.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError, requests.RequestException),
    )
    def _fetch_series(self, facet: str, start: str, end: str) -> list[dict[str, Any]]:
        """Fetch daily spot price data for a single EIA series facet."""
        params = {
            "api_key": self._api_key, "frequency": "daily",
            "data[0]": "value", "facets[series][]": facet,
            "start": start, "end": end, "length": 5000,
        }
        resp = requests.get(_EIA_BASE, params=params, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json().get("response", {}).get("data", [])

    def pull(self, days_back: int = 90) -> dict[str, Any]:
        """Pull recent EIA spot prices for Brent and WTI."""
        if not self._api_key:
            return {"status": "FAILED", "rows_inserted": 0, "error": "EIA_API_KEY not set"}

        end_str = date.today().isoformat()
        start_str = (date.today() - timedelta(days=days_back)).isoformat()
        total = 0

        for facet, suffix in _SERIES_MAP.items():
            sid = f"{_SERIES_PREFIX}.{suffix}"
            try:
                records = self._fetch_series(facet, start_str, end_str)
            except Exception as exc:
                log.error("EIA fetch failed for {f}: {e}", f=facet, e=str(exc))
                continue

            with self.engine.begin() as conn:
                existing = self._get_existing_dates(sid, conn)
                for rec in records:
                    val, period = rec.get("value"), rec.get("period", "")
                    if val is None or not period:
                        continue
                    try:
                        obs = date.fromisoformat(period)
                        fv = float(val)
                    except (ValueError, TypeError):
                        continue
                    if obs in existing:
                        continue
                    self._insert_raw(conn=conn, series_id=sid, obs_date=obs,
                                     value=fv, raw_payload={"facet": facet})
                    total += 1
            time.sleep(1.0)

        log.info("EIA: {n} rows inserted", n=total)
        return {"status": "SUCCESS", "rows_inserted": total}
