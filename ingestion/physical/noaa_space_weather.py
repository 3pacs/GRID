"""
GRID NOAA Space Weather ingestion module.

Pulls the planetary K-index (Kp) from NOAA SWPC and aggregates to
daily max. Kp >= 5 indicates geomagnetic storms that can disrupt
communications, GPS, and power grids.

Data source: https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json
No API key required.

Series stored:
- noaa_swpc.kp_daily_max: Daily maximum Kp value (0-9 scale)
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

_SWPC_KP_URL = "https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json"
_SERIES_PREFIX = "noaa_swpc"
_REQUEST_TIMEOUT: int = 30


class NOAASpaceWeatherPuller(BasePuller):
    """Pulls NOAA SWPC planetary K-index, aggregated to daily max."""

    SOURCE_NAME: str = "NOAA_SWPC"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://services.swpc.noaa.gov",
        "cost_tier": "FREE",
        "latency_class": "INTRADAY",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 35,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info("NOAASpaceWeatherPuller initialised -- source_id={sid}", sid=self.source_id)

    @retry_on_failure(
        max_attempts=3, backoff=2.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError, requests.RequestException),
    )
    def _fetch_kp_data(self) -> list:
        """Fetch Kp index JSON from NOAA SWPC."""
        resp = requests.get(_SWPC_KP_URL, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            raise ValueError("NOAA Kp response empty")
        return data

    def pull(self) -> dict[str, Any]:
        """Pull NOAA Kp index and store daily max values."""
        try:
            raw = self._fetch_kp_data()
        except Exception as exc:
            log.error("NOAA SWPC pull failed: {e}", e=str(exc))
            return {"status": "FAILED", "rows_inserted": 0, "error": str(exc)}

        daily_max: dict[date, float] = defaultdict(float)
        for row in raw:
            try:
                if isinstance(row, dict):
                    time_tag = str(row["time_tag"])
                    kp_val = float(row["Kp"])
                else:
                    time_tag = str(row[0])
                    kp_val = float(row[1])
                obs_date = datetime.fromisoformat(time_tag.replace("Z", "+00:00")).date()
            except (IndexError, ValueError, TypeError, KeyError):
                continue
            daily_max[obs_date] = max(daily_max[obs_date], kp_val)

        if not daily_max:
            log.warning("NOAA SWPC: no valid Kp observations parsed")
            return {"status": "SUCCESS", "rows_inserted": 0}

        total = 0
        sid = f"{_SERIES_PREFIX}.kp_daily_max"
        with self.engine.begin() as conn:
            existing = self._get_existing_dates(sid, conn)
            for obs_date, max_kp in sorted(daily_max.items()):
                if obs_date in existing:
                    continue
                self._insert_raw(conn=conn, series_id=sid, obs_date=obs_date,
                                 value=max_kp, raw_payload={"source_url": _SWPC_KP_URL})
                total += 1

        log.info("NOAA SWPC: {n} rows inserted", n=total)
        return {"status": "SUCCESS", "rows_inserted": total}
