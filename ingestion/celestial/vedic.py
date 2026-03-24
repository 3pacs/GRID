"""GRID -- Vedic (Jyotish) astrological data ingestion.

Computes Vedic astrology features from astronomical calculations.
All computations are deterministic (pure math) -- no external API required.

Features generated:
- nakshatra_index: current lunar mansion (0-26, 27 nakshatras)
- nakshatra_quality: 0=fixed, 1=movable, 2=dual (market implications in Jyotish)
- tithi: lunar day (1-30, waxing/waning)
- rahu_ketu_axis: position of shadow planets (eclipse axis, degrees)
- dasha_cycle_phase: Vimshottari dasha major period indicator (0-1)
"""

from __future__ import annotations

import math
from datetime import date, timedelta, datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller

# -- Astronomical constants --------------------------------------------------
SYNODIC_MONTH = 29.53059  # days
SIDEREAL_MONTH = 27.32166  # days (Moon's sidereal period)
_REF_NEW_MOON = datetime(2000, 1, 6, 18, 14, 0, tzinfo=timezone.utc)

# Moon's mean longitude at J2000 epoch (degrees) and daily rate
_MOON_L0 = 218.3165  # degrees at J2000
_MOON_RATE = 13.17640  # degrees per day (mean sidereal motion)

# Sun's mean longitude at J2000 epoch and daily rate
_SUN_L0 = 280.4665  # degrees at J2000
_SUN_RATE = 0.98565  # degrees per day

_J2000 = date(2000, 1, 1)

# Rahu (North Node) at J2000 and its regression rate
_RAHU_L0 = 125.0445  # degrees at J2000
_RAHU_RATE = -0.05295  # degrees per day (retrograde motion, 18.61-year cycle)

# -- Nakshatra definitions ---------------------------------------------------
# 27 nakshatras, each spanning 13 deg 20 min (13.3333 degrees)
# Quality mapping: 0=fixed (Dhruva), 1=movable (Chara), 2=dual (Dvisvabhava)
_NAKSHATRA_NAMES = [
    "Ashvini", "Bharani", "Krittika", "Rohini", "Mrigashira", "Ardra",
    "Punarvasu", "Pushya", "Ashlesha", "Magha", "Purva Phalguni",
    "Uttara Phalguni", "Hasta", "Chitra", "Svati", "Vishakha", "Anuradha",
    "Jyeshtha", "Mula", "Purva Ashadha", "Uttara Ashadha", "Shravana",
    "Dhanishta", "Shatabhisha", "Purva Bhadrapada", "Uttara Bhadrapada",
    "Revati",
]

# Quality per nakshatra: 0=fixed, 1=movable, 2=dual
_NAKSHATRA_QUALITY = [
    1, 0, 2, 0, 2, 0,  # Ashvini-Ardra
    2, 0, 1, 1, 0, 0,  # Punarvasu-Uttara Phalguni
    1, 2, 1, 2, 0, 0,  # Hasta-Jyeshtha
    0, 1, 0, 1, 1, 1,  # Mula-Shatabhisha
    2, 0, 1,            # Purva Bhadra-Revati
]

# -- Vimshottari Dasha -------------------------------------------------------
# 120-year cycle, planet periods in years (order matters)
_DASHA_PLANETS = ["Ketu", "Venus", "Sun", "Moon", "Mars",
                  "Rahu", "Jupiter", "Saturn", "Mercury"]
_DASHA_YEARS = [7, 20, 6, 10, 7, 18, 16, 19, 17]  # total = 120
_DASHA_TOTAL = 120  # years


def _moon_sidereal_longitude(d: date) -> float:
    """Return approximate sidereal longitude of the Moon in degrees (0-360)."""
    days = (d - _J2000).days
    # Mean sidereal longitude (simplified -- ignores perturbations)
    lon = (_MOON_L0 + _MOON_RATE * days) % 360.0
    return lon


def _sun_sidereal_longitude(d: date) -> float:
    """Return approximate sidereal longitude of the Sun in degrees (0-360).

    Uses tropical longitude minus ayanamsha (Lahiri approximation).
    """
    days = (d - _J2000).days
    # Tropical longitude
    tropical = (_SUN_L0 + _SUN_RATE * days) % 360.0
    # Lahiri ayanamsha: ~23.85 deg at J2000, increasing ~50.3 arcsec/year
    ayanamsha = 23.85 + (50.3 / 3600.0) * (days / 365.25)
    return (tropical - ayanamsha) % 360.0


def _nakshatra_from_longitude(lon: float) -> int:
    """Return nakshatra index (0-26) from sidereal longitude."""
    # Each nakshatra spans 360/27 = 13.3333... degrees
    return int(lon / (360.0 / 27.0)) % 27


def _tithi(d: date) -> int:
    """Return Vedic lunar day (tithi) from 1 to 30.

    Tithi is the angular distance between Moon and Sun divided by 12 degrees.
    """
    moon_lon = _moon_sidereal_longitude(d)
    sun_lon = _sun_sidereal_longitude(d)
    diff = (moon_lon - sun_lon) % 360.0
    tithi_num = int(diff / 12.0) + 1
    return min(tithi_num, 30)


def _rahu_longitude(d: date) -> float:
    """Return Rahu (North Node) sidereal longitude in degrees.

    Rahu moves retrograde with an 18.61-year cycle.
    """
    days = (d - _J2000).days
    return (_RAHU_L0 + _RAHU_RATE * days) % 360.0


def _dasha_cycle_phase(d: date) -> float:
    """Return position in the Vimshottari dasha cycle (0-1).

    Uses the Moon's nakshatra at birth of the cycle to seed the dasha.
    For market analysis, we use the current Moon nakshatra to derive
    a continuous cycle phase.
    """
    moon_lon = _moon_sidereal_longitude(d)
    # Each nakshatra is ruled by a dasha planet (cyclic mapping)
    nak_index = _nakshatra_from_longitude(moon_lon)
    # Position within nakshatra (0-1)
    nak_span = 360.0 / 27.0
    pos_in_nak = (moon_lon % nak_span) / nak_span
    # Map to dasha cycle: nakshatra index maps to planet index (mod 9)
    planet_idx = nak_index % 9
    # Compute cumulative years up to this planet
    cum_years = sum(_DASHA_YEARS[:planet_idx])
    # Add fractional position within this planet's period
    cum_years += pos_in_nak * _DASHA_YEARS[planet_idx]
    return cum_years / _DASHA_TOTAL


class VedicAstroPuller(BasePuller):
    """Computes Vedic (Jyotish) astrological features.

    All calculations are deterministic -- no external API needed.

    Attributes:
        SOURCE_NAME: 'VEDIC_JYOTISH' in source_catalog.
    """

    SOURCE_NAME = "VEDIC_JYOTISH"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "computed://vedic-jyotish",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "MED",
        "priority_rank": 92,
    }

    _SERIES = [
        "nakshatra_index",
        "nakshatra_quality",
        "tithi",
        "rahu_ketu_axis",
        "dasha_cycle_phase",
    ]

    def __init__(self, db_engine: Engine, lookback_days: int = 365) -> None:
        super().__init__(db_engine)
        self.lookback_days = lookback_days

    def _compute_day(self, d: date) -> dict[str, float]:
        """Compute all Vedic astrology features for a single date."""
        moon_lon = _moon_sidereal_longitude(d)
        nak_idx = _nakshatra_from_longitude(moon_lon)
        rahu_lon = _rahu_longitude(d)
        # Ketu is always 180 degrees from Rahu
        ketu_lon = (rahu_lon + 180.0) % 360.0

        return {
            "nakshatra_index": float(nak_idx),
            "nakshatra_quality": float(_NAKSHATRA_QUALITY[nak_idx]),
            "tithi": float(_tithi(d)),
            "rahu_ketu_axis": round(rahu_lon, 4),
            "dasha_cycle_phase": round(_dasha_cycle_phase(d), 6),
        }

    def pull_all(self, start_date: date | None = None) -> dict[str, Any]:
        """Generate Vedic astrology features for the configured lookback window.

        Parameters:
            start_date: Override for the first date to compute.
                        Defaults to today minus ``lookback_days``.

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
                    if self._row_exists("nakshatra_index", d, conn, dedup_hours=23):
                        d += timedelta(days=1)
                        continue

                    features = self._compute_day(d)
                    for series_id, value in features.items():
                        self._insert_raw(
                            conn=conn,
                            series_id=series_id,
                            obs_date=d,
                            value=value,
                            raw_payload={"source": "vedic_jyotish", "date": d.isoformat()},
                        )
                        rows_inserted += 1
                except Exception as exc:
                    errors += 1
                    log.warning(
                        "VedicAstroPuller error on {d}: {e}", d=d, e=str(exc)
                    )
                d += timedelta(days=1)

        status = "SUCCESS" if errors == 0 else "PARTIAL"
        log.info(
            "VedicAstroPuller complete -- {rows} rows, {errs} errors",
            rows=rows_inserted, errs=errors,
        )
        return {"rows_inserted": rows_inserted, "status": status}
