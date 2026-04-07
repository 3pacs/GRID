"""
GRID Open-Meteo weather data ingestion module.

Pulls daily weather observations for NYC from the Open-Meteo API.
Weather affects energy demand, agricultural output, retail foot
traffic, and transportation.

Data source: https://api.open-meteo.com/v1/forecast
No API key required.

Series stored:
- open_meteo.nyc_temp_max: Daily high temperature (Celsius)
- open_meteo.nyc_temp_min: Daily low temperature (Celsius)
- open_meteo.nyc_precip: Daily precipitation sum (mm)
"""

from __future__ import annotations

from datetime import date
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

_METEO_BASE = "https://api.open-meteo.com/v1/forecast"
_METEO_ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"
_SERIES_PREFIX = "open_meteo"
_REQUEST_TIMEOUT: int = 30
_NYC_LAT, _NYC_LON = 40.71, -74.01


class WeatherPuller(BasePuller):
    """Pulls daily weather data from Open-Meteo for NYC."""

    SOURCE_NAME: str = "open_meteo"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": _METEO_BASE,
        "cost_tier": "FREE",
        "latency_class": "DAILY",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "MED",
        "priority_rank": 45,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info("WeatherPuller initialised -- source_id={sid}", sid=self.source_id)

    @retry_on_failure(
        max_attempts=3, backoff=2.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError, requests.RequestException),
    )
    def _fetch_weather(self, past_days: int = 7) -> dict[str, Any]:
        """Fetch daily weather data for NYC."""
        params = {
            "latitude": _NYC_LAT, "longitude": _NYC_LON,
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
            "timezone": "America/New_York", "past_days": past_days,
        }
        resp = requests.get(_METEO_BASE, params=params, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json()

    def pull(self, past_days: int = 7) -> dict[str, Any]:
        """Pull weather data for NYC."""
        try:
            data = self._fetch_weather(past_days)
        except Exception as exc:
            log.error("Open-Meteo pull failed: {e}", e=str(exc))
            return {"status": "FAILED", "rows_inserted": 0, "error": str(exc)}

        daily = data.get("daily", {})
        dates = daily.get("time", [])
        t_max = daily.get("temperature_2m_max", [])
        t_min = daily.get("temperature_2m_min", [])
        precip = daily.get("precipitation_sum", [])

        total = 0
        features = {
            "nyc_temp_max": t_max, "nyc_temp_min": t_min, "nyc_precip": precip,
        }
        with self.engine.begin() as conn:
            for feat, vals in features.items():
                sid = f"{_SERIES_PREFIX}.{feat}"
                existing = self._get_existing_dates(sid, conn)
                for i, d_str in enumerate(dates):
                    try:
                        obs = date.fromisoformat(d_str)
                    except (ValueError, TypeError):
                        continue
                    v = vals[i] if i < len(vals) else None
                    if v is None or obs in existing:
                        continue
                    self._insert_raw(conn=conn, series_id=sid, obs_date=obs,
                                     value=float(v), raw_payload={"city": "nyc"})
                    total += 1

        log.info("Open-Meteo NYC: {n} rows inserted", n=total)
        return {"status": "SUCCESS", "rows_inserted": total}
