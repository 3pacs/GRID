"""
GRID NASA FIRMS fire data ingestion module.

Pulls active fire/hotspot data from NASA FIRMS. Uses VIIRS SNPP
near-real-time detections for the USA. Stores daily fire count and
average fire radiative power.

Data source: https://firms.modaps.eosdis.nasa.gov/api/country/csv/
Key from env NASA_FIRMS_KEY, fallback DEMO_KEY.

Series stored:
- nasa_firms.us_fire_count: Daily count of active fire detections (USA)
- nasa_firms.us_avg_frp: Daily average fire radiative power (MW)
"""

from __future__ import annotations

import io
import os
from collections import defaultdict
from datetime import date
from typing import Any

import pandas as pd
import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

_FIRMS_BASE = "https://firms.modaps.eosdis.nasa.gov/api/country/csv"
_SERIES_PREFIX = "nasa_firms"
_REQUEST_TIMEOUT: int = 60


class NASAFirePuller(BasePuller):
    """Pulls NASA FIRMS fire detections for the USA."""

    SOURCE_NAME: str = "nasa_firms"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": _FIRMS_BASE,
        "cost_tier": "FREE",
        "latency_class": "DAILY",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 40,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        from config import settings
        self._map_key = os.environ.get("NASA_FIRMS_KEY", "")
        self._earthdata_token = (
            getattr(settings, "NASA_EARTHDATA_TOKEN", "")
            or os.environ.get("NASA_EARTHDATA_TOKEN", "")
        )
        if not self._map_key and not self._earthdata_token:
            log.warning("NASA_FIRMS_KEY and NASA_EARTHDATA_TOKEN not set — FIRMS pull will fail")

    @retry_on_failure(
        max_attempts=3, backoff=3.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError, requests.RequestException),
    )
    def _fetch_csv(self, days: int = 1) -> pd.DataFrame:
        """Fetch FIRMS CSV for USA fire detections."""
        if self._map_key:
            url = f"{_FIRMS_BASE}/{self._map_key}/VIIRS_SNPP_NRT/USA/{days}"
            resp = requests.get(url, timeout=_REQUEST_TIMEOUT)
        else:
            # Use Earthdata token-based endpoint
            url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/VIIRS_SNPP_NRT/-130,24,-65,50/{days}"
            headers = {"Authorization": f"Bearer {self._earthdata_token}"}
            resp = requests.get(url, headers=headers, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        return pd.read_csv(io.StringIO(resp.text))

    def pull(self, days: int = 1) -> dict[str, Any]:
        """Pull NASA FIRMS data and aggregate daily fire stats."""
        try:
            df = self._fetch_csv(days=days)
        except Exception as exc:
            log.error("NASA FIRMS pull failed: {e}", e=str(exc))
            return {"status": "FAILED", "rows_inserted": 0, "error": str(exc)}
        if df.empty:
            return {"status": "SUCCESS", "rows_inserted": 0}

        dcol = next((c for c in df.columns if "acq_date" in c.lower()), None)
        if not dcol:
            return {"status": "FAILED", "rows_inserted": 0, "error": "No acq_date column"}
        df["_d"] = pd.to_datetime(df[dcol], errors="coerce")
        df = df.dropna(subset=["_d"])

        frp_col = next((c for c in df.columns if c.lower() == "frp"), None)
        counts: dict[date, int] = defaultdict(int)
        frp_sum: dict[date, float] = defaultdict(float)
        frp_n: dict[date, int] = defaultdict(int)
        for _, r in df.iterrows():
            d = r["_d"].date()
            counts[d] += 1
            if frp_col and pd.notna(r.get(frp_col)):
                try:
                    frp_sum[d] += float(r[frp_col])
                    frp_n[d] += 1
                except (ValueError, TypeError):
                    pass

        total = 0
        with self.engine.begin() as conn:
            sid_c = f"{_SERIES_PREFIX}.us_fire_count"
            sid_f = f"{_SERIES_PREFIX}.us_avg_frp"
            ex_c = self._get_existing_dates(sid_c, conn)
            ex_f = self._get_existing_dates(sid_f, conn)
            for d, cnt in sorted(counts.items()):
                if d not in ex_c:
                    self._insert_raw(conn=conn, series_id=sid_c, obs_date=d,
                                     value=float(cnt), raw_payload={"sensor": "VIIRS_SNPP_NRT"})
                    total += 1
                if d not in ex_f and frp_n.get(d, 0) > 0:
                    avg = round(frp_sum[d] / frp_n[d], 2)
                    self._insert_raw(conn=conn, series_id=sid_f, obs_date=d,
                                     value=avg, raw_payload={"sensor": "VIIRS_SNPP_NRT"})
                    total += 1

        log.info("NASA FIRMS: {n} rows inserted", n=total)
        return {"status": "SUCCESS", "rows_inserted": total}
