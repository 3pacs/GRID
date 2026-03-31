"""
GRID — Crucix bridge puller.

Reads Crucix's latest.json (or HTTP API) and maps all quantifiable signals
into GRID's raw_series taxonomy. This bridges ~25 Crucix intelligence sources
into GRID's orthogonality/clustering pipeline without duplicating API logic.

Crucix refreshes every 15 minutes. This puller extracts numeric time series
from each source and stores them as raw_series rows for resolution/features.

Source mapping:
    GSCPI           → supply chain pressure index
    Treasury        → total debt, public debt, interest rates
    Telegram        → urgent post count, total posts, channel reach
    USAspending     → defense contract volume
    NOAA            → severe weather alerts
    EPA             → radiation readings
    Safecast        → nuclear site radiation levels
    Patents         → filing counts by tech domain
    Maritime        → chokepoint metadata
    WHO             → disease outbreak count
    OpenSanctions   → sanctions entity counts
    Bluesky         → social topic mention counts
    KiwiSDR         → radio network health
    FRED (Crucix)   → skipped (GRID pulls FRED directly)
    yFinance        → skipped (GRID pulls yFinance directly)
"""
from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller

# Default path to Crucix latest.json on grid-svr
CRUCIX_LATEST_PATH = "/data/grid_v4/Crucix/runs/latest.json"

# Sources GRID already pulls directly — skip to avoid duplicates
_SKIP_SOURCES = {"FRED", "YFinance", "BLS", "Comtrade"}


class CrucixBridgePuller(BasePuller):
    """Ingests Crucix intelligence data into GRID's raw_series.

    Reads from Crucix's latest.json file (local) or HTTP endpoint
    and extracts all quantifiable signals into GRID's taxonomy.

    Attributes:
        crucix_path: Path to Crucix's latest.json file.
        crucix_url: Optional HTTP URL to Crucix API (fallback).
    """

    SOURCE_NAME: str = "Crucix"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "http://localhost:3117",
        "cost_tier": "FREE",
        "latency_class": "INTRADAY",
        "pit_available": False,
        "revision_behavior": "FREQUENT",
        "trust_score": "MED",
        "priority_rank": 40,
    }

    def __init__(
        self,
        db_engine: Engine,
        crucix_path: str = CRUCIX_LATEST_PATH,
        crucix_url: str | None = None,
    ) -> None:
        self.crucix_path = crucix_path
        self.crucix_url = crucix_url or "http://localhost:3117"
        super().__init__(db_engine)
        log.info(
            "CrucixBridgePuller initialised — source_id={sid}, path={p}",
            sid=self.source_id,
            p=self.crucix_path,
        )

    def _load_latest(self) -> dict[str, Any]:
        """Load Crucix data from file or HTTP."""
        # Try file first (faster, no network)
        p = Path(self.crucix_path)
        if p.exists():
            data = json.loads(p.read_text())
            ts = data.get("crucix", {}).get("timestamp", "")
            log.info("Loaded Crucix data from file — timestamp={ts}", ts=ts)
            return data

        # Fallback to HTTP
        try:
            import urllib.request
            with urllib.request.urlopen(self.crucix_url, timeout=30) as resp:
                data = json.loads(resp.read().decode())
                log.info("Loaded Crucix data from HTTP")
                return data
        except Exception as exc:
            log.error("Failed to load Crucix data: {e}", e=str(exc))
            return {}

    def pull_all(self) -> dict[str, Any]:
        """Extract all quantifiable signals from Crucix into raw_series.

        Returns:
            Summary dict with rows_inserted, sources_processed counts.
        """
        data = self._load_latest()
        if not data:
            return {"status": "FAILED", "rows_inserted": 0, "error": "No Crucix data"}

        sources = data.get("sources", {})
        today = date.today()
        total_inserted = 0
        sources_processed = 0

        with self.engine.begin() as conn:
            existing = self._get_existing_dates_multi(conn, today)

            for source_name, source_data in sources.items():
                if source_name in _SKIP_SOURCES:
                    continue

                extractor = _EXTRACTORS.get(source_name)
                if not extractor:
                    continue

                try:
                    signals = extractor(source_data)
                    for series_id, value in signals.items():
                        if value is None:
                            continue
                        full_id = f"crucix.{series_id}"
                        if full_id in existing:
                            continue
                        try:
                            numeric_val = float(value)
                        except (ValueError, TypeError):
                            continue
                        conn.execute(
                            text(
                                "INSERT INTO raw_series "
                                "(series_id, source_id, obs_date, value, pull_status) "
                                "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                            ),
                            {
                                "sid": full_id,
                                "src": self.source_id,
                                "od": today,
                                "val": numeric_val,
                            },
                        )
                        total_inserted += 1
                    sources_processed += 1
                except Exception as exc:
                    log.warning(
                        "Crucix extractor failed for {s}: {e}",
                        s=source_name,
                        e=str(exc),
                    )

        log.info(
            "Crucix bridge — {ins} rows from {src} sources",
            ins=total_inserted,
            src=sources_processed,
        )
        return {
            "status": "SUCCESS",
            "rows_inserted": total_inserted,
            "sources_processed": sources_processed,
        }

    def _get_existing_dates_multi(self, conn: Any, obs_date: date) -> set[str]:
        """Get all crucix.* series_ids already stored for today."""
        rows = conn.execute(
            text(
                "SELECT DISTINCT series_id FROM raw_series "
                "WHERE source_id = :src AND obs_date = :od "
                "AND series_id LIKE 'crucix.%' AND pull_status = 'SUCCESS'"
            ),
            {"src": self.source_id, "od": obs_date},
        ).fetchall()
        return {r[0] for r in rows}


# ---------------------------------------------------------------------------
# Source extractors — each returns {series_id: numeric_value}
# ---------------------------------------------------------------------------

def _extract_gscpi(data: dict) -> dict[str, float | None]:
    """NY Fed Global Supply Chain Pressure Index."""
    latest = data.get("latest", {})
    history = data.get("history", [])
    signals: dict[str, float | None] = {}
    if latest.get("value") is not None:
        signals["gscpi.value"] = latest["value"]
    # Month-over-month change
    if len(history) >= 2:
        curr = history[0].get("value")
        prev = history[1].get("value")
        if curr is not None and prev is not None:
            signals["gscpi.mom_change"] = curr - prev
    return signals


def _extract_treasury(data: dict) -> dict[str, float | None]:
    """US Treasury debt and interest rates."""
    signals: dict[str, float | None] = {}
    debt = data.get("debt", [])
    if debt:
        latest = debt[0]
        try:
            signals["treasury.total_debt_trn"] = float(latest.get("totalDebt", 0)) / 1e12
            signals["treasury.public_debt_trn"] = float(latest.get("publicDebt", 0)) / 1e12
            signals["treasury.intragov_debt_trn"] = float(latest.get("intragovDebt", 0)) / 1e12
        except (ValueError, TypeError):
            pass
        if len(debt) >= 2:
            try:
                curr = float(debt[0].get("totalDebt", 0))
                prev = float(debt[1].get("totalDebt", 0))
                signals["treasury.debt_daily_change_bn"] = (curr - prev) / 1e9
            except (ValueError, TypeError):
                pass

    rates = data.get("interestRates", [])
    for rate in rates:
        key = rate.get("type", "").lower().replace(" ", "_").replace("-", "_")
        if key and rate.get("avgRate") is not None:
            signals[f"treasury.rate_{key}"] = rate["avgRate"]
    return signals


def _extract_telegram(data: dict) -> dict[str, float | None]:
    """Telegram intelligence channel monitoring."""
    signals: dict[str, float | None] = {}
    signals["telegram.channels_monitored"] = data.get("channelsMonitored")
    signals["telegram.channels_reachable"] = data.get("channelsReachable")
    signals["telegram.total_posts"] = data.get("totalPosts")
    urgent = data.get("urgentPosts", [])
    signals["telegram.urgent_post_count"] = len(urgent) if urgent else 0
    return signals


def _extract_usaspending(data: dict) -> dict[str, float | None]:
    """USAspending defense contract data."""
    signals: dict[str, float | None] = {}
    contracts = data.get("recentDefenseContracts", [])
    signals["usaspending.defense_contract_count"] = len(contracts)
    total_value = sum(
        float(c.get("amount", 0))
        for c in contracts
        if c.get("amount") is not None
    )
    signals["usaspending.defense_contract_total_bn"] = total_value / 1e9
    return signals


def _extract_noaa(data: dict) -> dict[str, float | None]:
    """NOAA severe weather alerts."""
    signals: dict[str, float | None] = {}
    signals["noaa.severe_alerts_total"] = data.get("totalSevereAlerts")
    summary = data.get("summary", {})
    for key in ("hurricanes", "tornadoes", "floods", "winterStorms", "wildfires"):
        signals[f"noaa.{key}"] = summary.get(key)
    return signals


def _extract_epa(data: dict) -> dict[str, float | None]:
    """EPA RadNet radiation monitoring."""
    signals: dict[str, float | None] = {}
    signals["epa.radnet_total_readings"] = data.get("totalReadings")
    readings = data.get("readings", [])
    if readings:
        values = [r.get("value", 0) for r in readings if r.get("value") is not None]
        if values:
            signals["epa.radnet_avg_reading"] = sum(values) / len(values)
            signals["epa.radnet_max_reading"] = max(values)
    return signals


def _extract_safecast(data: dict) -> dict[str, float | None]:
    """Safecast nuclear radiation monitoring."""
    signals: dict[str, float | None] = {}
    sites = data.get("sites", [])
    for site in sites:
        key = site.get("key", "unknown")
        signals[f"safecast.{key}_avg_cpm"] = site.get("avgCPM")
        signals[f"safecast.{key}_max_cpm"] = site.get("maxCPM")
        signals[f"safecast.{key}_anomaly"] = 1.0 if site.get("anomaly") else 0.0
    return signals


def _extract_patents(data: dict) -> dict[str, float | None]:
    """USPTO patent filing counts by strategic domain."""
    signals: dict[str, float | None] = {}
    signals["patents.total_found"] = data.get("totalFound")
    recent = data.get("recentPatents", {})
    for domain, patents in recent.items():
        signals[f"patents.{domain}_count"] = len(patents) if isinstance(patents, list) else 0
    return signals


def _extract_maritime(data: dict) -> dict[str, float | None]:
    """Maritime chokepoint vessel tracking."""
    signals: dict[str, float | None] = {}
    chokepoints = data.get("chokepoints", {})
    for key, cp in chokepoints.items():
        if isinstance(cp, dict):
            vessels = cp.get("vesselCount") or cp.get("totalVessels")
            if vessels is not None:
                signals[f"maritime.{key}_vessels"] = vessels
    return signals


def _extract_who(data: dict) -> dict[str, float | None]:
    """WHO disease outbreak monitoring."""
    signals: dict[str, float | None] = {}
    dons = data.get("diseaseOutbreakNews", [])
    signals["who.outbreak_news_count"] = len(dons)
    return signals


def _extract_opensanctions(data: dict) -> dict[str, float | None]:
    """OpenSanctions entity counts."""
    signals: dict[str, float | None] = {}
    signals["opensanctions.total_entities"] = data.get("totalSanctionedEntities")
    searches = data.get("recentSearches", [])
    for s in searches:
        query = s.get("query", "").lower().replace(" ", "_")
        if query:
            signals[f"opensanctions.{query}_results"] = s.get("totalResults", 0)
    return signals


def _extract_bluesky(data: dict) -> dict[str, float | None]:
    """Bluesky social sentiment counts."""
    signals: dict[str, float | None] = {}
    topics = data.get("topics", {})
    for topic, posts in topics.items():
        count = len(posts) if isinstance(posts, list) else 0
        signals[f"bluesky.{topic}_posts"] = count
    return signals


def _extract_kiwisdr(data: dict) -> dict[str, float | None]:
    """KiwiSDR radio network monitoring."""
    signals: dict[str, float | None] = {}
    network = data.get("network", {})
    signals["kiwisdr.total_receivers"] = network.get("totalReceivers")
    signals["kiwisdr.online_receivers"] = network.get("onlineReceivers")
    geographic = data.get("geographic", {})
    if isinstance(geographic, dict):
        signals["kiwisdr.countries_covered"] = len(geographic)
    return signals


def _extract_opensky(data: dict) -> dict[str, float | None]:
    """OpenSky ADS-B military/surveillance flight tracking."""
    signals: dict[str, float | None] = {}
    hotspots = data.get("hotspots", [])
    total_aircraft = 0
    for hs in hotspots:
        key = hs.get("key", "unknown")
        count = hs.get("totalAircraft", 0)
        signals[f"opensky.{key}_aircraft"] = count
        total_aircraft += count
    signals["opensky.total_hotspot_aircraft"] = total_aircraft
    return signals


def _extract_adsb(data: dict) -> dict[str, float | None]:
    """ADS-B Exchange military aircraft tracking."""
    signals: dict[str, float | None] = {}
    mil = data.get("militaryAircraft", [])
    signals["adsb.military_aircraft_count"] = len(mil) if isinstance(mil, list) else 0
    return signals


def _extract_acled(data: dict) -> dict[str, float | None]:
    """ACLED armed conflict event data."""
    signals: dict[str, float | None] = {}
    events = data.get("events", [])
    signals["acled.event_count"] = len(events) if isinstance(events, list) else 0
    fatalities = sum(e.get("fatalities", 0) for e in events if isinstance(e, dict))
    signals["acled.fatalities"] = fatalities
    return signals


def _extract_reliefweb(data: dict) -> dict[str, float | None]:
    """ReliefWeb/HDX humanitarian data."""
    signals: dict[str, float | None] = {}
    datasets = data.get("hdxDatasets", [])
    signals["reliefweb.hdx_dataset_count"] = len(datasets) if isinstance(datasets, list) else 0
    return signals


def _extract_space(data: dict) -> dict[str, float | None]:
    """Space weather monitoring."""
    signals: dict[str, float | None] = {}
    solar = data.get("solarFlares", [])
    signals["space.solar_flare_count"] = len(solar) if isinstance(solar, list) else 0
    geo = data.get("geomagneticStorms", [])
    signals["space.geomagnetic_storm_count"] = len(geo) if isinstance(geo, list) else 0
    kp = data.get("kpIndex")
    if kp is not None:
        signals["space.kp_index"] = kp
    return signals


# Registry of extractors keyed by Crucix source name
_EXTRACTORS: dict[str, Any] = {
    "GSCPI": _extract_gscpi,
    "Treasury": _extract_treasury,
    "Telegram": _extract_telegram,
    "USAspending": _extract_usaspending,
    "NOAA": _extract_noaa,
    "EPA": _extract_epa,
    "Safecast": _extract_safecast,
    "Patents": _extract_patents,
    "Maritime": _extract_maritime,
    "WHO": _extract_who,
    "OpenSanctions": _extract_opensanctions,
    "Bluesky": _extract_bluesky,
    "KiwiSDR": _extract_kiwisdr,
    "OpenSky": _extract_opensky,
    "ADS-B": _extract_adsb,
    "ACLED": _extract_acled,
    "ReliefWeb": _extract_reliefweb,
    "Space": _extract_space,
}


if __name__ == "__main__":
    from db import get_engine
    engine = get_engine()
    puller = CrucixBridgePuller(db_engine=engine)
    result = puller.pull_all()
    print(f"Crucix bridge: {result}")
