"""
NASA FIRMS puller — active fire/thermal anomaly detection from satellites.

Tracks wildfires, industrial fires, and volcanic activity globally.
Fires near critical infrastructure (refineries, ports, data centers,
power plants) are market-moving events.

API: https://firms.modaps.eosdis.nasa.gov/api/area
Authentication is the FIRMS MAP_KEY in the URL path
(``/api/area/csv/{MAP_KEY}/{SOURCE}/{w,s,e,n}/{days}``), read from
``NASA_FIRMS_KEY`` -- the same variable ``ingestion/physical/nasa_fire.py``
uses. The previous request omitted the MAP_KEY and sent the Earthdata JWT as
a Bearer header; FIRMS answered ``400 Invalid MAP_KEY`` for every region
(probed from grid-svr 2026-11-04), so this puller had stored nothing.

Satellite: VIIRS_SNPP_NRT stopped at 2026-11-02 in FIRMS' own
data_availability listing while NOAA-20/NOAA-21 are current, so the puller
asks ``/api/data_availability`` which NRT product is fresh and uses the first
current one in ``_SOURCE_PREFERENCE``. A region count from a stale product
would be a false zero, so no current product means FAILED, not 0 fires.
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

FIRMS_API = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
# MAP_KEY endpoint for FIRMS (simpler, uses MAP_KEY not Earthdata JWT)
FIRMS_MAP_API = "https://firms.modaps.eosdis.nasa.gov/api/country/csv"
FIRMS_AVAILABILITY_API = "https://firms.modaps.eosdis.nasa.gov/api/data_availability/csv"

# Fresh-first NRT products (all serve the same CSV columns for our use).
_SOURCE_PREFERENCE: tuple[str, ...] = (
    "VIIRS_NOAA20_NRT", "VIIRS_NOAA21_NRT", "VIIRS_SNPP_NRT", "MODIS_NRT",
)
# A product whose max_date is older than this is not "current".
_MAX_PRODUCT_LAG_DAYS = 1

# Permanent HTTP codes for FIRMS. 400 usually means a bbox is outside
# the supported range for the source (MODIS_NRT vs VIIRS_SNPP_NRT differ),
# and the next call would fail identically. 401/403 = token invalid.
# 404/410 = endpoint gone. All non-retryable.
_HTTP_PERMANENT_CODES: frozenset[int] = frozenset({400, 401, 403, 404, 410})


class FIRMSPermanentError(RuntimeError):
    """Raised on permanent HTTP codes from NASA FIRMS.

    Non-retryable: not a subclass of requests.RequestException, so
    retry_on_failure's retryable tuple doesn't match. Caller logs
    WARNING once and moves on to the next region.
    """

# Key regions to monitor (bounding boxes: west, south, east, north)
REGIONS: dict[str, tuple[float, float, float, float]] = {
    "us_west_coast": (-125.0, 32.0, -114.0, 49.0),      # CA, OR, WA wildfires
    "us_gulf_coast": (-98.0, 25.0, -80.0, 31.0),         # TX, LA refineries
    "us_east_coast": (-80.0, 25.0, -65.0, 45.0),         # Financial hubs
    "amazon": (-75.0, -15.0, -45.0, 5.0),                # Brazil deforestation
    "europe_industrial": (-10.0, 35.0, 30.0, 55.0),      # EU industry
    "middle_east_oil": (35.0, 12.0, 60.0, 42.0),         # Oil infrastructure
    "southeast_asia": (95.0, -10.0, 140.0, 25.0),        # Manufacturing hubs
    "australia": (110.0, -45.0, 155.0, -10.0),            # Mining + fires
    "siberia": (60.0, 50.0, 180.0, 75.0),                # Methane + wildfires
}

# Critical infrastructure proximity alerts (lat, lon, radius_km, name)
CRITICAL_INFRA: list[tuple[float, float, float, str]] = [
    (29.76, -95.37, 50, "Houston Refinery Complex"),
    (30.00, -90.17, 50, "Louisiana Chemical Corridor"),
    (33.94, -118.41, 30, "Port of Los Angeles"),
    (37.77, -122.42, 30, "San Francisco Tech Hub"),
    (40.71, -74.01, 30, "NYC Financial District"),
    (51.51, -0.13, 30, "London Financial District"),
    (25.20, 55.27, 50, "Dubai / Jebel Ali Port"),
    (1.35, 103.82, 30, "Singapore Port"),
    (31.23, 121.47, 50, "Shanghai Industrial Zone"),
    (35.68, 139.69, 30, "Tokyo Metro"),
]


class NASAFirmsPuller(BasePuller):
    """Pull active fire data from NASA FIRMS satellites."""

    SOURCE_NAME = "nasa_firms"
    SOURCE_CONFIG = {
        "base_url": "https://firms.modaps.eosdis.nasa.gov",
        "cost_tier": "FREE",
        "latency_class": "REALTIME",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 25,
    }

    def __init__(self, db_engine: Engine, token: str | None = None, map_key: str | None = None) -> None:
        super().__init__(db_engine)
        if token:
            self.token = token
        else:
            from config import settings
            self.token = getattr(settings, "NASA_EARTHDATA_TOKEN", "")
        self.map_key = map_key if map_key is not None else os.environ.get("NASA_FIRMS_KEY", "")

    def _redact(self, text_: str) -> str:
        return text_.replace(self.map_key, "***") if self.map_key else text_

    def _current_source(self, today: date) -> str | None:
        """First preferred NRT product whose data reaches ``today - lag``.

        Returns None if FIRMS lists no current product. If the availability
        listing itself cannot be read, falls back to the first preference
        (the area request will then show whether it works).
        """
        try:
            resp = requests.get(f"{FIRMS_AVAILABILITY_API}/{self.map_key}/ALL", timeout=30)
            if resp.status_code != 200:
                raise FIRMSPermanentError(f"data_availability HTTP {resp.status_code}")
            max_dates: dict[str, date] = {}
            for line in resp.text.strip().split("\n")[1:]:
                parts = line.strip().split(",")
                if len(parts) >= 3:
                    try:
                        max_dates[parts[0]] = date.fromisoformat(parts[2])
                    except ValueError:
                        continue
        except Exception as exc:
            log.warning("FIRMS availability listing unavailable ({e}); using {s}",
                        e=self._redact(str(exc)), s=_SOURCE_PREFERENCE[0])
            return _SOURCE_PREFERENCE[0]
        cutoff = today - timedelta(days=_MAX_PRODUCT_LAG_DAYS)
        for source in _SOURCE_PREFERENCE:
            if max_dates.get(source) is not None and max_dates[source] >= cutoff:
                return source
        return None

    @retry_on_failure(max_attempts=3, retryable_exceptions=(ConnectionError, TimeoutError, OSError, requests.exceptions.RequestException))
    def _fetch_fires(
        self,
        west: float, south: float, east: float, north: float,
        days: int = 2,
        source: str = _SOURCE_PREFERENCE[0],
    ) -> list[dict[str, Any]]:
        """Fetch fire data for a bounding box.

        Args:
            west, south, east, north: Bounding box coordinates.
            days: Number of days of data (FIRMS accepts 1..5).
            source: FIRMS NRT product (see ``_SOURCE_PREFERENCE``).

        Returns:
            List of fire point dicts.
        """
        url = f"{FIRMS_API}/{self.map_key}/{source}/{west},{south},{east},{north}/{days}"
        try:
            resp = requests.get(url, timeout=60)
        except requests.exceptions.RequestException as exc:
            # requests puts the URL (and so the MAP_KEY) in its messages;
            # the retry wrapper logs str(exc), so re-raise it redacted.
            raise ConnectionError(self._redact(f"{type(exc).__name__}: {exc}")) from None
        if resp.status_code in _HTTP_PERMANENT_CODES:
            # Never put the URL in the message: the MAP_KEY is a path segment.
            raise FIRMSPermanentError(
                f"FIRMS area {source} returned {resp.status_code}: "
                f"{self._redact(resp.text.strip()[:120])}"
            )
        if resp.status_code >= 400:
            raise requests.exceptions.HTTPError(f"FIRMS area {source} returned {resp.status_code}")

        # Parse CSV response. A valid answer always starts with the header
        # row, even with zero fires; anything else is an error page.
        lines = resp.text.strip().split("\n")
        if not lines or not lines[0].startswith("latitude,"):
            raise FIRMSPermanentError(
                f"FIRMS area {source} returned no CSV header: {self._redact(lines[0][:120] if lines else '')}"
            )
        if len(lines) < 2:
            return []

        headers_row = lines[0].split(",")
        fires: list[dict[str, Any]] = []
        for line in lines[1:]:
            values = line.split(",")
            if len(values) >= len(headers_row):
                fire = dict(zip(headers_row, values))
                fires.append(fire)

        return fires

    def pull(self, days: int = 2) -> dict[str, Any]:
        """Pull fire data for all monitored regions.

        Args:
            days: Days of data to fetch (FIRMS accepts 1..5).

        Returns:
            Write-contract summary: ``status``, ``rows_inserted`` (rows
            committed to raw_series) and ``succeeded``/``total`` regions.
        """
        if not self.map_key:
            return {"status": "SKIPPED", "rows_inserted": 0,
                    "skipped_reason": "NASA_FIRMS_KEY not set (FIRMS MAP_KEY required)"}

        today = date.today()
        source = self._current_source(today)
        if source is None:
            return {"status": "FAILED", "rows_inserted": 0,
                    "error": f"no FIRMS NRT product has data newer than {today - timedelta(days=_MAX_PRODUCT_LAG_DAYS)}"}

        total_fires = 0
        rows_inserted = 0
        infra_alerts: list[dict[str, Any]] = []
        region_counts: dict[str, int] = {}
        failed: list[str] = []

        for idx, (region_name, bbox) in enumerate(REGIONS.items()):
            if idx:
                time.sleep(1.0)  # Rate limit
            try:
                fires = self._fetch_fires(*bbox, days=days, source=source)
                with self.engine.begin() as conn:
                    n = _inserted(self._insert_raw(
                        conn,
                        series_id=f"firms:{region_name}:fire_count",
                        obs_date=today,
                        value=float(len(fires)),
                        raw_payload={"region": region_name, "days": days,
                                     "bbox": list(bbox), "satellite": source},
                        return_result=True,
                    ))
                rows_inserted += n
            except FIRMSPermanentError as exc:
                failed.append(region_name)
                log.warning("FIRMS {r}: permanent failure, skipping region — {e}",
                            r=region_name, e=self._redact(str(exc)))
                continue
            except Exception as exc:
                failed.append(region_name)
                log.warning("FIRMS pull failed for {r}: {e}", r=region_name, e=self._redact(str(exc)))
                continue

            region_counts[region_name] = len(fires)
            total_fires += len(fires)

            # Check proximity to critical infrastructure
            for fire in fires:
                try:
                    lat = float(fire.get("latitude", 0))
                    lon = float(fire.get("longitude", 0))
                    brightness = float(fire.get("bright_ti4", fire.get("brightness", 0)))
                    confidence = fire.get("confidence", "")
                except (ValueError, TypeError):
                    continue
                for infra_lat, infra_lon, radius_km, infra_name in CRITICAL_INFRA:
                    dist = _haversine(lat, lon, infra_lat, infra_lon)
                    if dist <= radius_km:
                        infra_alerts.append({
                            "infrastructure": infra_name,
                            "fire_lat": lat,
                            "fire_lon": lon,
                            "distance_km": round(dist, 1),
                            "brightness": brightness,
                            "confidence": confidence,
                            "region": region_name,
                        })
                        log.warning(
                            "FIRE NEAR INFRA: {n} — {d:.0f}km away, brightness={b}",
                            n=infra_name, d=dist, b=brightness,
                        )

        if infra_alerts:
            try:
                with self.engine.begin() as conn:
                    n = _inserted(self._insert_raw(
                        conn,
                        series_id="firms:infra_alerts",
                        obs_date=today,
                        value=float(len(infra_alerts)),
                        raw_payload={"alerts": infra_alerts, "satellite": source},
                        return_result=True,
                    ))
                rows_inserted += n
            except Exception as exc:
                failed.append("infra_alerts")
                log.warning("FIRMS infra alert store failed: {e}", e=str(exc))

        log.info("NASA FIRMS ({s}): {t} fires across {r} regions, {a} infrastructure alerts, {n} rows",
                 s=source, t=total_fires, r=len(region_counts), a=len(infra_alerts), n=rows_inserted)

        total = len(REGIONS)
        succeeded = len(region_counts)
        if not failed:
            status = "SUCCESS"
        elif rows_inserted:
            status = "PARTIAL"
        else:
            status = "FAILED"
        result: dict[str, Any] = {
            "status": status,
            "rows_inserted": rows_inserted,
            "succeeded": succeeded,
            "total": total,
            "satellite": source,
            "total_fires": total_fires,
            "region_counts": region_counts,
            "infrastructure_alerts": infra_alerts,
        }
        if failed:
            result["error"] = f"{len(failed)} FIRMS items failed: {', '.join(failed)}"
        return result


def _inserted(result: Any) -> int:
    """Committed-row contribution of one INSERT; unknown counts fail closed."""
    affected = getattr(result, "rowcount", None)
    if type(affected) is not int or affected < 0:
        raise ValueError("Unknown affected row count")
    return affected


def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Compute distance in km between two lat/lon points."""
    import math
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
