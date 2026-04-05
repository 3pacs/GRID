"""
NASA FIRMS puller — active fire/thermal anomaly detection from satellites.

Tracks wildfires, industrial fires, and volcanic activity globally.
Fires near critical infrastructure (refineries, ports, data centers,
power plants) are market-moving events.

API: https://firms.modaps.eosdis.nasa.gov/api/area
Uses NASA Earthdata JWT for authentication.
"""

from __future__ import annotations

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

    def __init__(self, db_engine: Engine, token: str | None = None) -> None:
        super().__init__(db_engine)
        if token:
            self.token = token
        else:
            from config import settings
            self.token = getattr(settings, "NASA_EARTHDATA_TOKEN", "")

    @retry_on_failure(max_attempts=3, retryable_exceptions=(ConnectionError, TimeoutError, OSError, requests.exceptions.RequestException))
    def _fetch_fires(
        self,
        west: float, south: float, east: float, north: float,
        days: int = 2,
        source: str = "VIIRS_SNPP_NRT",
    ) -> list[dict[str, Any]]:
        """Fetch fire data for a bounding box.

        Args:
            west, south, east, north: Bounding box coordinates.
            days: Number of days of data (1-10).
            source: Satellite source (VIIRS_SNPP_NRT, MODIS_NRT).

        Returns:
            List of fire point dicts.
        """
        url = f"{FIRMS_API}/{source}/{west},{south},{east},{north}/{days}"
        headers = {}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        resp = requests.get(url, headers=headers, timeout=60)
        resp.raise_for_status()

        # Parse CSV response
        lines = resp.text.strip().split("\n")
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
            days: Days of data to fetch.

        Returns:
            Summary with fire counts and infrastructure alerts.
        """
        total_fires = 0
        infra_alerts: list[dict[str, Any]] = []
        region_counts: dict[str, int] = {}

        for region_name, bbox in REGIONS.items():
            try:
                fires = self._fetch_fires(*bbox, days=days)
                time.sleep(1.0)  # Rate limit

                region_counts[region_name] = len(fires)
                total_fires += len(fires)

                # Store regional fire count
                with self.engine.begin() as conn:
                    self._insert_raw(conn,
                        series_id=f"firms:{region_name}:fire_count",
                        obs_date=date.today(),
                        value=float(len(fires)),
                        raw_payload={"region": region_name, "days": days, "bbox": list(bbox)},
                    )

                # Check proximity to critical infrastructure
                for fire in fires:
                    try:
                        lat = float(fire.get("latitude", 0))
                        lon = float(fire.get("longitude", 0))
                        brightness = float(fire.get("bright_ti4", fire.get("brightness", 0)))
                        confidence = fire.get("confidence", "")

                        for infra_lat, infra_lon, radius_km, infra_name in CRITICAL_INFRA:
                            dist = _haversine(lat, lon, infra_lat, infra_lon)
                            if dist <= radius_km:
                                alert = {
                                    "infrastructure": infra_name,
                                    "fire_lat": lat,
                                    "fire_lon": lon,
                                    "distance_km": round(dist, 1),
                                    "brightness": brightness,
                                    "confidence": confidence,
                                    "region": region_name,
                                }
                                infra_alerts.append(alert)
                                log.warning(
                                    "FIRE NEAR INFRA: {n} — {d:.0f}km away, brightness={b}",
                                    n=infra_name, d=dist, b=brightness,
                                )
                    except (ValueError, TypeError):
                        pass

            except Exception as exc:
                log.debug("FIRMS pull failed for {r}: {e}", r=region_name, e=str(exc))

        # Store infrastructure alerts
        if infra_alerts:
            with self.engine.begin() as conn:
                self._insert_raw(conn,
                    series_id="firms:infra_alerts",
                    obs_date=date.today(),
                    value=float(len(infra_alerts)),
                    raw_payload={"alerts": infra_alerts},
                )

        log.info("NASA FIRMS: {t} fires across {r} regions, {a} infrastructure alerts",
                 t=total_fires, r=len(region_counts), a=len(infra_alerts))

        return {
            "total_fires": total_fires,
            "region_counts": region_counts,
            "infrastructure_alerts": infra_alerts,
        }


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
