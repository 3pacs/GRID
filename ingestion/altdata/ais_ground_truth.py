"""
GRID AIS Ground-Truth Port Presence ingestion module.

This is the **ground-truth observation layer** for shipping statistics.
It counts the physical vessels actually present at each of ~15 strategic
global ports via public AIS (Automatic Identification System) data, so
that we can cross-check the *reported* numbers coming out of:

    - CAT-51   LME warehouse stocks
    - CAT-52   Chinese iron-ore port stocks (Mysteel / Custeel)
    - CAT-82   Drewry WCI / SCFI / FBX container freight indices

against the *observed* physical reality at the berth. It is the spiritual
counterpart to ``intelligence/cross_reference.py`` (government stats vs
physical reality), extended from macro indicators to port traffic.

**The signal is simple.** If Mysteel says Chinese port iron-ore stocks
are rising but AIS shows 30% fewer Capesize ore-carriers at berth in
Qingdao, then the reported data is stale, fudged, or mis-classified.
The converse is equally actionable.

This module produces the independent observation layer only. The
downstream consumer — ``intelligence/shipping_fudge_detector.py`` (to
be built separately) — computes the divergence against reported stats
and emits a ``shipping_fudge`` signal when the gap exceeds a configured
trust-weighted threshold.

Data strategy (all free / public)
---------------------------------
1. **VesselFinder public port page HTML** — primary path.
   URL: ``https://www.vesselfinder.com/ports/{code}``
   Each port page has three sections — "In Port", "Expected Arrivals",
   "Departed" — with a vessel count in the section header. No API key,
   no registration, just HTML parsing with BeautifulSoup. Polite pacing
   at ~0.5s between port fetches is mandatory.

2. **AISHub free CSV feed** — secondary fallback.
   URL: ``https://www.aishub.net/api`` (requires free ``AISHUB_API_KEY``
   env var; academic / research tier is free). Gracefully skipped when
   the env var is not set.

3. **aisstream.io free tier** — *future enhancement*. WebSocket live
   stream, rate-limited to 500 msgs/s. Not suitable for a periodic
   puller; will be wired in for a real-time mode later.

Scheduling
----------
Intended cadence: every 4 hours via the Hermes scheduler. Each run
walks ``AIS_PORTS`` and tries VesselFinder first, AISHub second, and
reports per-port success/failure. No single port's failure blocks the
run.

Series written to ``raw_series``
--------------------------------
For each port_slug, up to five series are written:

    ais:ships_at_berth:<port_slug>
    ais:ships_at_anchor:<port_slug>
    ais:ships_expected:<port_slug>
    ais:ships_departed_24h:<port_slug>
    ais:capacity_utilization:<port_slug>

Idempotency is enforced by rounding the observation timestamp down to
the nearest 4-hour bucket (``_round_to_bucket``) and skipping any
(series_id, bucket_date) combination already present in ``raw_series``
for the same source. Re-running inside the same 4h window is a no-op.
"""

from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller

# ── Configuration ────────────────────────────────────────────────────

VESSELFINDER_URL_FMT: str = "https://www.vesselfinder.com/ports/{code}"
AISHUB_API_URL: str = "https://www.aishub.net/api"

SOURCE_PRIORITY: tuple[str, ...] = ("vesselfinder", "aishub")

_REQUEST_TIMEOUT: int = 20
_PORT_FETCH_DELAY_S: float = 0.5
_BUCKET_HOURS: int = 4
_USER_AGENT: str = (
    "Mozilla/5.0 (compatible; GRID-AIS-GroundTruth/1.0; "
    "+https://grid.stepdad.finance)"
)


# ── Data classes ─────────────────────────────────────────────────────


@dataclass(frozen=True)
class PortSpec:
    """Metadata for a single global port tracked by the puller.

    Attributes:
        slug: Short lowercase identifier used in ``series_id``.
        display_name: Human-readable port name.
        country: ISO-3166-alpha-2 country code.
        vesselfinder_code: VesselFinder port code (e.g. ``"CNQIN001"``)
            or ``None`` if unknown / unreachable.
        lat: Port latitude (decimal degrees, WGS-84).
        lng: Port longitude (decimal degrees, WGS-84).
        bounding_box: ``(min_lat, min_lng, max_lat, max_lng)`` for
            AIS bounding-box queries.
    """

    slug: str
    display_name: str
    country: str
    vesselfinder_code: str | None
    lat: float
    lng: float
    bounding_box: tuple[float, float, float, float]


@dataclass(frozen=True)
class AISSnapshot:
    """A single point-in-time observation of vessel presence at a port.

    Attributes:
        timestamp: UTC datetime of observation (rounded to 4h bucket).
        port_slug: The ``PortSpec.slug`` this observation belongs to.
        ships_at_berth: Count of vessels currently berthed / loading.
        ships_at_anchor: Count of vessels at anchor / waiting.
        ships_expected: Count of vessels with an ETA for this port.
        ships_departed_24h: Count of vessels departed in last 24 hours.
        capacity_utilization: ``at_berth / (at_berth + at_anchor)``,
            clamped to ``[0, 1]``, or ``None`` when both are zero.
        source: Provenance label — ``"vesselfinder"``, ``"aishub"``,
            or ``"none"``.
    """

    timestamp: datetime
    port_slug: str
    ships_at_berth: int
    ships_at_anchor: int
    ships_expected: int
    ships_departed_24h: int
    capacity_utilization: float | None
    source: str


# ── Port catalog ─────────────────────────────────────────────────────

AIS_PORTS: tuple[PortSpec, ...] = (
    # Chinese iron-ore + container hubs (cross-check CAT-52, CAT-82)
    PortSpec("qingdao",       "Qingdao",         "CN", "CNQIN001", 36.07,  120.35, (35.9,  120.15, 36.25, 120.55)),
    PortSpec("shanghai",      "Shanghai",        "CN", "CNSHA001", 31.23,  121.47, (30.9,  121.2,  31.6,  122.0)),
    PortSpec("ningbo",        "Ningbo-Zhoushan", "CN", "CNNGB001", 29.86,  121.55, (29.6,  121.3,  30.2,  122.0)),
    PortSpec("tianjin",       "Tianjin",         "CN", "CNTSN001", 38.98,  117.78, (38.85, 117.6,  39.15, 118.0)),
    # US West + East (cross-check CAT-82, congestion proxy)
    PortSpec("la",            "Los Angeles",     "US", "USLAX001", 33.74, -118.27, (33.6,  -118.4, 33.9,  -118.1)),
    PortSpec("long_beach",    "Long Beach",      "US", "USLGB001", 33.77, -118.21, (33.65, -118.35,33.9,  -118.05)),
    PortSpec("ny_nj",         "NY/NJ",           "US", "USNYC001", 40.67, -74.05,  (40.55, -74.2,  40.8,  -73.9)),
    # European main container gateways
    PortSpec("rotterdam",     "Rotterdam",       "NL", "NLRTM001", 51.95,  4.14,   (51.85, 3.95,   52.1,  4.35)),
    PortSpec("antwerp",       "Antwerp",         "BE", "BEANR001", 51.26,  4.39,   (51.15, 4.25,   51.4,  4.55)),
    PortSpec("hamburg",       "Hamburg",         "DE", "DEHAM001", 53.55,  9.98,   (53.45, 9.8,    53.65, 10.15)),
    # SE Asia pivot
    PortSpec("singapore",     "Singapore",       "SG", "SGSIN001", 1.27,   103.84, (1.15,  103.65, 1.4,   104.05)),
    PortSpec("port_klang",    "Port Klang",      "MY", "MYPKG001", 3.0,    101.39, (2.9,   101.25, 3.15,  101.55)),
    # Middle East chokepoint
    PortSpec("jebel_ali",     "Jebel Ali",       "AE", "AEJEA001", 24.98,  55.06,  (24.85, 54.9,   25.1,  55.25)),
    PortSpec("jeddah",        "Jeddah",          "SA", "SAJED001", 21.49,  39.18,  (21.35, 39.05,  21.65, 39.35)),
    # Taiwan Strait witness (cross-check CAT-91)
    PortSpec("kaohsiung",     "Kaohsiung",       "TW", "TWKHH001", 22.62,  120.28, (22.5,  120.1,  22.8,  120.5)),
)


# ── Pure helpers ─────────────────────────────────────────────────────


def compute_capacity_utilization(at_berth: int, at_anchor: int) -> float | None:
    """Return the ratio of berthed ships to total (berthed + anchored).

    Returns ``None`` when both counts are zero (no observation possible).
    Otherwise returns a float in ``[0, 1]``.

    Parameters:
        at_berth: Number of vessels currently at berth.
        at_anchor: Number of vessels currently at anchor.

    Returns:
        Utilization ratio in ``[0, 1]`` or ``None`` if no vessels.
    """
    if at_berth < 0 or at_anchor < 0:
        return None
    total = at_berth + at_anchor
    if total <= 0:
        return None
    util = at_berth / float(total)
    if util < 0.0:
        return 0.0
    if util > 1.0:
        return 1.0
    return util


def _round_to_bucket(ts: datetime, bucket_hours: int = _BUCKET_HOURS) -> datetime:
    """Round a UTC timestamp *down* to the nearest N-hour bucket.

    Used to enforce 4h idempotency: two runs inside the same bucket
    produce the same bucket timestamp, so the DB upsert is a no-op.
    """
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    else:
        ts = ts.astimezone(timezone.utc)
    hour_bucket = (ts.hour // bucket_hours) * bucket_hours
    return ts.replace(hour=hour_bucket, minute=0, second=0, microsecond=0)


_SECTION_PATTERNS: dict[str, tuple[re.Pattern[str], ...]] = {
    "at_berth": (
        re.compile(r"in\s*port[^0-9]{0,40}(\d{1,5})", re.IGNORECASE),
        re.compile(r"(\d{1,5})\s*vessels?\s*in\s*port", re.IGNORECASE),
        re.compile(r"currently\s*in\s*port[^0-9]{0,40}(\d{1,5})", re.IGNORECASE),
    ),
    "at_anchor": (
        re.compile(r"at\s*anchor[^0-9]{0,40}(\d{1,5})", re.IGNORECASE),
        re.compile(r"(\d{1,5})\s*vessels?\s*at\s*anchor", re.IGNORECASE),
        re.compile(r"anchored[^0-9]{0,40}(\d{1,5})", re.IGNORECASE),
    ),
    "expected": (
        re.compile(r"expected\s*arrivals?[^0-9]{0,40}(\d{1,5})", re.IGNORECASE),
        re.compile(r"(\d{1,5})\s*vessels?\s*expected", re.IGNORECASE),
        re.compile(r"arrivals?[^0-9]{0,40}(\d{1,5})", re.IGNORECASE),
    ),
    "departed": (
        re.compile(r"departed[^0-9]{0,40}(\d{1,5})", re.IGNORECASE),
        re.compile(r"(\d{1,5})\s*vessels?\s*departed", re.IGNORECASE),
        re.compile(r"departures?[^0-9]{0,40}(\d{1,5})", re.IGNORECASE),
    ),
}


def _extract_count(text_blob: str, keys: tuple[re.Pattern[str], ...]) -> int:
    """Return the first integer match from a list of regex patterns."""
    for pattern in keys:
        match = pattern.search(text_blob)
        if match:
            try:
                return int(match.group(1))
            except (ValueError, IndexError):
                continue
    return 0


def _parse_vesselfinder_html(html: str, port: PortSpec) -> AISSnapshot | None:
    """Parse a VesselFinder port page into an ``AISSnapshot``.

    Returns ``None`` on empty / malformed input or when no section counts
    could be extracted (all zero). Never raises.

    The VesselFinder port page structure we target looks like:

        <section class="in-port">
            <h2>In Port</h2>
            <span class="count">12 vessels</span>
            ...

    We're permissive: we find ``<section>``, ``<div>``, ``<article>``,
    or ``<h2>`` elements whose text contains one of the labels
    ("in port", "at anchor", "expected arrivals", "departed") and then
    extract the first integer in that block. Falls back to whole-page
    regex scan when section tags are missing.

    Parameters:
        html: Raw HTML string from VesselFinder.
        port: ``PortSpec`` the HTML belongs to (for tagging the snapshot).

    Returns:
        ``AISSnapshot`` or ``None``.
    """
    if not html or not isinstance(html, str):
        return None

    try:
        from bs4 import BeautifulSoup
    except ImportError:
        log.warning("BeautifulSoup not installed — falling back to regex only")
        soup = None
    else:
        try:
            soup = BeautifulSoup(html, "html.parser")
        except Exception as exc:  # noqa: BLE001 — parser may throw anything
            log.warning(
                "BeautifulSoup parse failed for {slug}: {e}",
                slug=port.slug,
                e=str(exc),
            )
            soup = None

    at_berth = 0
    at_anchor = 0
    expected = 0
    departed = 0

    # Section-level extraction via BS4 if available
    if soup is not None:
        section_tags = soup.find_all(
            ["section", "div", "article", "h2", "h3", "header"]
        )
        for tag in section_tags:
            try:
                block_text = tag.get_text(separator=" ", strip=True)
            except Exception:  # noqa: BLE001
                continue
            if not block_text:
                continue
            block_lower = block_text.lower()

            # "In Port" section
            if at_berth == 0 and "in port" in block_lower:
                at_berth = _extract_count(block_text, _SECTION_PATTERNS["at_berth"])
            # "At Anchor" section (often nested inside "in port")
            if at_anchor == 0 and (
                "at anchor" in block_lower or "anchored" in block_lower
            ):
                at_anchor = _extract_count(block_text, _SECTION_PATTERNS["at_anchor"])
            # "Expected Arrivals"
            if expected == 0 and (
                "expected" in block_lower or "arrivals" in block_lower
            ):
                expected = _extract_count(block_text, _SECTION_PATTERNS["expected"])
            # "Departed"
            if departed == 0 and (
                "departed" in block_lower or "departures" in block_lower
            ):
                departed = _extract_count(block_text, _SECTION_PATTERNS["departed"])

    # Whole-page fallback regex scan (catches pages without structural tags)
    if at_berth == 0:
        at_berth = _extract_count(html, _SECTION_PATTERNS["at_berth"])
    if at_anchor == 0:
        at_anchor = _extract_count(html, _SECTION_PATTERNS["at_anchor"])
    if expected == 0:
        expected = _extract_count(html, _SECTION_PATTERNS["expected"])
    if departed == 0:
        departed = _extract_count(html, _SECTION_PATTERNS["departed"])

    if at_berth == 0 and at_anchor == 0 and expected == 0 and departed == 0:
        return None

    util = compute_capacity_utilization(at_berth, at_anchor)
    return AISSnapshot(
        timestamp=_round_to_bucket(datetime.now(timezone.utc)),
        port_slug=port.slug,
        ships_at_berth=at_berth,
        ships_at_anchor=at_anchor,
        ships_expected=expected,
        ships_departed_24h=departed,
        capacity_utilization=util,
        source="vesselfinder",
    )


# ── Puller ───────────────────────────────────────────────────────────


class AISGroundTruthPuller(BasePuller):
    """Periodic AIS ground-truth port-presence puller.

    Walks ``AIS_PORTS`` and fetches per-port vessel counts from
    VesselFinder (primary) and AISHub (secondary). Writes one
    ``AISSnapshot`` per port per 4-hour bucket into ``raw_series``
    across five series namespaces per port.

    Attributes:
        engine: SQLAlchemy engine.
        source_id: Resolved ``source_catalog.id`` for ais_ground_truth.
        aishub_api_key: AISHub API key (empty string when unset).
    """

    SOURCE_NAME: str = "ais_ground_truth"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://www.vesselfinder.com/",
        "cost_tier": "FREE",
        "latency_class": "INTRADAY",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 20,
    }

    def __init__(
        self,
        db_engine: Engine,
        aishub_api_key: str = "",
        api_key: str = "",
    ) -> None:
        self.aishub_api_key = aishub_api_key or api_key or os.environ.get(
            "AISHUB_API_KEY", ""
        )
        super().__init__(db_engine)
        log.info(
            "AISGroundTruthPuller initialised — source_id={sid}, aishub_key={k}, ports={n}",
            sid=self.source_id,
            k="set" if self.aishub_api_key else "missing",
            n=len(AIS_PORTS),
        )

    # ------------------------------------------------------------------ #
    # VesselFinder path
    # ------------------------------------------------------------------ #

    def _fetch_vesselfinder_html(self, port: PortSpec) -> str | None:
        """Fetch the VesselFinder port page HTML for a single port.

        Returns ``None`` on any failure (network, non-200, missing code).
        Never raises.
        """
        if port.vesselfinder_code is None:
            return None
        url = VESSELFINDER_URL_FMT.format(code=port.vesselfinder_code)
        headers = {
            "User-Agent": _USER_AGENT,
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
            ),
            "Accept-Language": "en-US,en;q=0.5",
        }
        try:
            resp = requests.get(url, headers=headers, timeout=_REQUEST_TIMEOUT)
        except requests.RequestException as exc:
            log.warning(
                "vesselfinder fetch failed for {slug}: {e}",
                slug=port.slug,
                e=str(exc),
            )
            return None
        if resp.status_code != 200:
            log.warning(
                "vesselfinder non-200 for {slug}: {s}",
                slug=port.slug,
                s=resp.status_code,
            )
            return None
        return resp.text

    def _pull_vesselfinder(self, port: PortSpec) -> AISSnapshot | None:
        """Fetch + parse the VesselFinder page for a single port."""
        html = self._fetch_vesselfinder_html(port)
        if html is None:
            return None
        return _parse_vesselfinder_html(html, port)

    # ------------------------------------------------------------------ #
    # AISHub path
    # ------------------------------------------------------------------ #

    def _pull_aishub(self, port: PortSpec) -> AISSnapshot | None:
        """Fetch AIS data from AISHub for a single port bounding box.

        Gracefully returns ``None`` when ``AISHUB_API_KEY`` is not set.
        Never raises.
        """
        if not self.aishub_api_key:
            return None
        (min_lat, min_lng, max_lat, max_lng) = port.bounding_box
        params = {
            "username": self.aishub_api_key,
            "format": "1",
            "output": "json",
            "compress": "0",
            "latmin": min_lat,
            "latmax": max_lat,
            "lonmin": min_lng,
            "lonmax": max_lng,
        }
        try:
            resp = requests.get(
                AISHUB_API_URL, params=params, timeout=_REQUEST_TIMEOUT
            )
        except requests.RequestException as exc:
            log.warning(
                "aishub fetch failed for {slug}: {e}",
                slug=port.slug,
                e=str(exc),
            )
            return None
        if resp.status_code != 200:
            log.warning(
                "aishub non-200 for {slug}: {s}",
                slug=port.slug,
                s=resp.status_code,
            )
            return None
        try:
            data = resp.json()
        except ValueError as exc:
            log.warning(
                "aishub JSON parse failed for {slug}: {e}",
                slug=port.slug,
                e=str(exc),
            )
            return None

        # AISHub returns a list whose element[0] is a status header and
        # element[1] is the vessel list. We're lenient about the shape.
        vessels: list[dict[str, Any]] = []
        if isinstance(data, list) and len(data) >= 2 and isinstance(data[1], list):
            vessels = [v for v in data[1] if isinstance(v, dict)]
        elif isinstance(data, list):
            vessels = [v for v in data if isinstance(v, dict)]
        elif isinstance(data, dict):
            vessels = list(data.get("vessels", []))

        if not vessels:
            return None

        at_berth = 0
        at_anchor = 0
        for v in vessels:
            nav_status = str(v.get("NAVSTAT", v.get("navstat", ""))).lower()
            speed_raw = v.get("SOG", v.get("sog", 0))
            try:
                speed = float(speed_raw)
            except (TypeError, ValueError):
                speed = 0.0
            # NAVSTAT 1 and 5 = at anchor / moored; speed < 0.3 kn = stationary
            if "anchor" in nav_status or nav_status in {"1", "5"}:
                at_anchor += 1
            elif speed < 0.3:
                at_berth += 1
            else:
                at_berth += 1 if speed < 2.0 else 0

        util = compute_capacity_utilization(at_berth, at_anchor)
        return AISSnapshot(
            timestamp=_round_to_bucket(datetime.now(timezone.utc)),
            port_slug=port.slug,
            ships_at_berth=at_berth,
            ships_at_anchor=at_anchor,
            ships_expected=0,
            ships_departed_24h=0,
            capacity_utilization=util,
            source="aishub",
        )

    # ------------------------------------------------------------------ #
    # Orchestration
    # ------------------------------------------------------------------ #

    def pull(self) -> list[AISSnapshot]:
        """Walk ``AIS_PORTS`` and return one snapshot per port (or skip).

        Never raises. Polite ``_PORT_FETCH_DELAY_S`` pause between ports.
        """
        snapshots: list[AISSnapshot] = []
        for port in AIS_PORTS:
            snap: AISSnapshot | None = None
            for source_name in SOURCE_PRIORITY:
                try:
                    if source_name == "vesselfinder":
                        snap = self._pull_vesselfinder(port)
                    elif source_name == "aishub":
                        snap = self._pull_aishub(port)
                except Exception as exc:  # noqa: BLE001
                    log.warning(
                        "{src} raised for {slug}: {e}",
                        src=source_name,
                        slug=port.slug,
                        e=str(exc),
                    )
                    snap = None
                if snap is not None:
                    break
            if snap is not None:
                snapshots.append(snap)
            # polite pacing even on failure so we don't hammer either host
            try:
                time.sleep(_PORT_FETCH_DELAY_S)
            except Exception:  # noqa: BLE001 — sleep mock may misbehave
                pass
        return snapshots

    # ------------------------------------------------------------------ #
    # Persistence
    # ------------------------------------------------------------------ #

    def save_to_db(self, snapshots: list[AISSnapshot]) -> int:
        """Upsert snapshots into ``raw_series``.

        Returns the number of rows inserted (5 per snapshot at most,
        minus any that were deduped by an existing row in the same bucket).
        """
        if not snapshots:
            return 0

        inserted = 0
        with self.engine.begin() as conn:
            for snap in snapshots:
                series_map = {
                    f"ais:ships_at_berth:{snap.port_slug}": snap.ships_at_berth,
                    f"ais:ships_at_anchor:{snap.port_slug}": snap.ships_at_anchor,
                    f"ais:ships_expected:{snap.port_slug}": snap.ships_expected,
                    f"ais:ships_departed_24h:{snap.port_slug}": snap.ships_departed_24h,
                }
                if snap.capacity_utilization is not None:
                    series_map[f"ais:capacity_utilization:{snap.port_slug}"] = (
                        float(snap.capacity_utilization)
                    )

                obs_date: date = snap.timestamp.date()

                for series_id, value in series_map.items():
                    if self._bucket_row_exists(conn, series_id, snap.timestamp):
                        continue
                    try:
                        self._insert_raw(
                            conn,
                            series_id=series_id,
                            obs_date=obs_date,
                            value=float(value),
                            raw_payload={
                                "port_slug": snap.port_slug,
                                "source": snap.source,
                                "bucket_ts": snap.timestamp.isoformat(),
                                "ships_at_berth": snap.ships_at_berth,
                                "ships_at_anchor": snap.ships_at_anchor,
                                "ships_expected": snap.ships_expected,
                                "ships_departed_24h": snap.ships_departed_24h,
                                "capacity_utilization": snap.capacity_utilization,
                            },
                        )
                        inserted += 1
                    except Exception as exc:  # noqa: BLE001
                        log.warning(
                            "insert failed series={sid} port={p}: {e}",
                            sid=series_id,
                            p=snap.port_slug,
                            e=str(exc),
                        )
        return inserted

    def _bucket_row_exists(
        self,
        conn: Any,
        series_id: str,
        bucket_ts: datetime,
    ) -> bool:
        """Check whether a row already exists in the current 4h bucket.

        Prevents duplicate inserts when the puller is re-run inside the
        same bucket window (e.g. operator manually triggers).
        """
        bucket_start = bucket_ts
        bucket_end = bucket_ts + timedelta(hours=_BUCKET_HOURS)
        try:
            result = conn.execute(
                text(
                    "SELECT 1 FROM raw_series "
                    "WHERE series_id = :sid AND source_id = :src "
                    "AND pull_timestamp >= :start "
                    "AND pull_timestamp < :end LIMIT 1"
                ),
                {
                    "sid": series_id,
                    "src": self.source_id,
                    "start": bucket_start,
                    "end": bucket_end,
                },
            ).fetchone()
            return result is not None
        except Exception as exc:  # noqa: BLE001
            log.debug(
                "bucket_row_exists check failed ({e}) — assuming not present",
                e=str(exc),
            )
            return False


# ── Module-level runner ──────────────────────────────────────────────


def run_ais_ground_truth_puller(engine: Engine) -> dict[str, Any]:
    """Single-shot runner invoked by the Hermes scheduler.

    Always returns a summary payload, even when every port fails.
    """
    try:
        puller = AISGroundTruthPuller(engine)
    except Exception as exc:  # noqa: BLE001
        log.error("AISGroundTruthPuller init failed: {e}", e=str(exc))
        return {
            "fetched": 0,
            "inserted": 0,
            "ports_scraped": [],
            "ports_failed": [p.slug for p in AIS_PORTS],
            "source_mix": {"vesselfinder": 0, "aishub": 0, "none": len(AIS_PORTS)},
            "error": str(exc),
        }

    snapshots: list[AISSnapshot] = []
    try:
        snapshots = puller.pull()
    except Exception as exc:  # noqa: BLE001
        log.error("AISGroundTruthPuller.pull() crashed: {e}", e=str(exc))

    scraped_slugs = {s.port_slug for s in snapshots}
    ports_scraped = [s.port_slug for s in snapshots]
    ports_failed = [p.slug for p in AIS_PORTS if p.slug not in scraped_slugs]

    source_mix: dict[str, int] = {"vesselfinder": 0, "aishub": 0, "none": 0}
    for snap in snapshots:
        key = snap.source if snap.source in source_mix else "none"
        source_mix[key] += 1
    source_mix["none"] = len(ports_failed)

    inserted = 0
    try:
        inserted = puller.save_to_db(snapshots)
    except Exception as exc:  # noqa: BLE001
        log.error("AISGroundTruthPuller.save_to_db() failed: {e}", e=str(exc))

    return {
        "fetched": len(snapshots),
        "inserted": inserted,
        "ports_scraped": ports_scraped,
        "ports_failed": ports_failed,
        "source_mix": source_mix,
    }
