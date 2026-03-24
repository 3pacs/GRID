"""GRID — Planetary aspect data ingestion.

Computes planetary positions and aspects using simplified Keplerian orbital
elements.  No external API needed — all calculations are deterministic.

Features generated:
- mercury_retrograde: 1.0 when Mercury is retrograde, 0.0 otherwise
- jupiter_saturn_angle: angular separation (0-180 degrees)
- mars_volatility_index: composite of Mars aspects (Gann theory)
- planetary_stress_index: count of hard aspects active
- venus_cycle_phase: Venus synodic cycle position (0-1)
"""

from __future__ import annotations

import math
from datetime import date, timedelta
from typing import Any

from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller

# ── Mercury retrograde periods 2020-2030 (start, end) ──────────────
# Source: US Naval Observatory / JPL Horizons pre-computed
_MERCURY_RETROGRADES: list[tuple[date, date]] = [
    # 2020
    (date(2020, 2, 17), date(2020, 3, 10)),
    (date(2020, 6, 18), date(2020, 7, 12)),
    (date(2020, 10, 14), date(2020, 11, 3)),
    # 2021
    (date(2021, 1, 30), date(2021, 2, 21)),
    (date(2021, 5, 29), date(2021, 6, 22)),
    (date(2021, 9, 27), date(2021, 10, 18)),
    # 2022
    (date(2022, 1, 14), date(2022, 2, 4)),
    (date(2022, 5, 10), date(2022, 6, 3)),
    (date(2022, 9, 10), date(2022, 10, 2)),
    (date(2022, 12, 29), date(2023, 1, 18)),
    # 2023
    (date(2023, 4, 21), date(2023, 5, 15)),
    (date(2023, 8, 23), date(2023, 9, 15)),
    (date(2023, 12, 13), date(2024, 1, 2)),
    # 2024
    (date(2024, 4, 1), date(2024, 4, 25)),
    (date(2024, 8, 5), date(2024, 8, 28)),
    (date(2024, 11, 26), date(2024, 12, 15)),
    # 2025
    (date(2025, 3, 15), date(2025, 4, 7)),
    (date(2025, 7, 18), date(2025, 8, 11)),
    (date(2025, 11, 9), date(2025, 11, 29)),
    # 2026
    (date(2026, 2, 26), date(2026, 3, 20)),
    (date(2026, 6, 29), date(2026, 7, 23)),
    (date(2026, 10, 24), date(2026, 11, 13)),
    # 2027
    (date(2027, 2, 9), date(2027, 3, 3)),
    (date(2027, 6, 10), date(2027, 7, 4)),
    (date(2027, 10, 7), date(2027, 10, 28)),
    # 2028
    (date(2028, 1, 24), date(2028, 2, 14)),
    (date(2028, 5, 21), date(2028, 6, 13)),
    (date(2028, 9, 19), date(2028, 10, 11)),
    # 2029
    (date(2029, 1, 7), date(2029, 1, 27)),
    (date(2029, 5, 1), date(2029, 5, 25)),
    (date(2029, 9, 2), date(2029, 9, 24)),
    (date(2029, 12, 22), date(2030, 1, 11)),
    # 2030
    (date(2030, 4, 13), date(2030, 5, 7)),
    (date(2030, 8, 16), date(2030, 9, 8)),
    (date(2030, 12, 5), date(2030, 12, 25)),
]

# ── Simplified orbital elements (mean longitude at J2000 + daily rate) ──
# Used for approximate geocentric longitude calculations
# Format: (L0_deg, rate_deg_per_day)
_ORBITAL_ELEMENTS: dict[str, tuple[float, float]] = {
    "mercury": (252.251, 4.09233),
    "venus": (181.980, 1.60213),
    "earth": (100.464, 0.98560),
    "mars": (355.453, 0.52403),
    "jupiter": (34.351, 0.08309),
    "saturn": (50.077, 0.03346),
}

# J2000 epoch
_J2000 = date(2000, 1, 1)


def _helio_longitude(planet: str, d: date) -> float:
    """Return approximate heliocentric ecliptic longitude in degrees."""
    L0, rate = _ORBITAL_ELEMENTS[planet]
    days = (d - _J2000).days
    return (L0 + rate * days) % 360.0


def _geo_longitude(planet: str, d: date) -> float:
    """Return approximate geocentric longitude (helio minus Earth)."""
    if planet == "earth":
        return 0.0
    h_planet = _helio_longitude(planet, d)
    h_earth = _helio_longitude("earth", d)
    return (h_planet - h_earth) % 360.0


def _angular_separation(lon1: float, lon2: float) -> float:
    """Return angular separation in degrees (0-180)."""
    diff = abs(lon1 - lon2) % 360.0
    return diff if diff <= 180.0 else 360.0 - diff


def _is_mercury_retrograde(d: date) -> bool:
    """Check if Mercury is retrograde on date *d* using precomputed table."""
    for start, end in _MERCURY_RETROGRADES:
        if start <= d <= end:
            return True
    # Fallback for dates outside the table: approximate using synodic period
    # Mercury is retrograde ~21% of the time, ~24 days per 115.88-day cycle
    if d < date(2020, 1, 1) or d > date(2030, 12, 31):
        ref_start = date(2020, 2, 17)
        days_since = (d - ref_start).days
        cycle_pos = days_since % 115.88
        # Retrograde roughly days 0-24 of each cycle
        return cycle_pos < 24.0
    return False


def _hard_aspect_count(d: date) -> int:
    """Count 'hard' aspects (conjunction, opposition, square) among planets.

    A hard aspect is when angular separation is within 8 degrees of
    0 (conjunction), 90 (square), or 180 (opposition).
    """
    planets = ["mercury", "venus", "mars", "jupiter", "saturn"]
    lons = {p: _geo_longitude(p, d) for p in planets}
    count = 0
    orb = 8.0  # degrees
    for i, p1 in enumerate(planets):
        for p2 in planets[i + 1:]:
            sep = _angular_separation(lons[p1], lons[p2])
            if sep < orb or abs(sep - 90) < orb or abs(sep - 180) < orb:
                count += 1
    return count


def _venus_synodic_phase(d: date) -> float:
    """Return Venus synodic cycle phase (0-1).

    Venus synodic period = 583.9 days.
    Reference inferior conjunction: 2022-01-09.
    """
    ref = date(2022, 1, 9)
    period = 583.9
    days = (d - ref).days
    return (days % period) / period


def _mars_volatility_index(d: date) -> float:
    """Composite Mars aspect metric inspired by Gann theory.

    Returns a 0-1 score based on Mars angular relationships with
    Jupiter and Saturn (traditionally associated with volatility).
    """
    mars_lon = _geo_longitude("mars", d)
    jup_lon = _geo_longitude("jupiter", d)
    sat_lon = _geo_longitude("saturn", d)

    mars_jup = _angular_separation(mars_lon, jup_lon)
    mars_sat = _angular_separation(mars_lon, sat_lon)

    # Score: higher when Mars is in hard aspect to Jupiter or Saturn
    score = 0.0
    for sep in [mars_jup, mars_sat]:
        for target in [0, 90, 180]:
            closeness = abs(sep - target)
            if closeness < 15:
                score += (15 - closeness) / 15.0
    return min(score / 2.0, 1.0)  # Normalise to 0-1


class PlanetaryAspectPuller(BasePuller):
    """Computes planetary aspect features from orbital mechanics.

    All calculations are deterministic — no external API needed.

    Attributes:
        SOURCE_NAME: 'PLANETARY_EPHEMERIS' in source_catalog.
    """

    SOURCE_NAME = "PLANETARY_EPHEMERIS"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "computed://orbital-mechanics",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "MED",
        "priority_rank": 91,
    }

    _SERIES = [
        "mercury_retrograde",
        "jupiter_saturn_angle",
        "mars_volatility_index",
        "planetary_stress_index",
        "venus_cycle_phase",
    ]

    def __init__(self, db_engine: Engine, lookback_days: int = 365) -> None:
        super().__init__(db_engine)
        self.lookback_days = lookback_days

    def _compute_day(self, d: date) -> dict[str, float]:
        """Compute all planetary features for a single date."""
        jup_lon = _geo_longitude("jupiter", d)
        sat_lon = _geo_longitude("saturn", d)

        return {
            "mercury_retrograde": 1.0 if _is_mercury_retrograde(d) else 0.0,
            "jupiter_saturn_angle": round(_angular_separation(jup_lon, sat_lon), 4),
            "mars_volatility_index": round(_mars_volatility_index(d), 6),
            "planetary_stress_index": float(_hard_aspect_count(d)),
            "venus_cycle_phase": round(_venus_synodic_phase(d), 6),
        }

    def pull_all(self, start_date: date | None = None) -> dict[str, Any]:
        """Generate planetary features for the configured lookback window.

        Parameters:
            start_date: Override for the first date to compute.

        Returns:
            dict with 'rows_inserted' and 'status' keys.
        """
        today = date.today()
        if start_date is None:
            start_date = today - timedelta(days=self.lookback_days)

        rows_inserted = 0
        errors = 0

        with self.engine.begin() as conn:
            d = start_date
            while d <= today:
                try:
                    if self._row_exists("mercury_retrograde", d, conn, dedup_hours=23):
                        d += timedelta(days=1)
                        continue

                    features = self._compute_day(d)
                    for series_id, value in features.items():
                        self._insert_raw(
                            conn=conn,
                            series_id=series_id,
                            obs_date=d,
                            value=value,
                            raw_payload={"source": "orbital_mechanics", "date": d.isoformat()},
                        )
                        rows_inserted += 1
                except Exception as exc:
                    errors += 1
                    log.warning(
                        "PlanetaryAspectPuller error on {d}: {e}", d=d, e=str(exc)
                    )
                d += timedelta(days=1)

        status = "SUCCESS" if errors == 0 else "PARTIAL"
        log.info(
            "PlanetaryAspectPuller complete — {rows} rows, {errs} errors",
            rows=rows_inserted, errs=errors,
        )
        return {"rows_inserted": rows_inserted, "status": status}
