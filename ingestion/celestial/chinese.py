"""GRID -- Chinese calendar and Feng Shui data ingestion.

Computes Chinese calendar features from traditional algorithms.
All computations are deterministic (pure math) -- no external API required.

Features generated:
- chinese_zodiac_year: animal year index (0-11: Rat through Pig)
- chinese_element: element index (0-4: Wood, Fire, Earth, Metal, Water)
- chinese_yin_yang: 0=yang, 1=yin
- feng_shui_flying_star: current period star number (1-9)
- chinese_lunar_month: 1-12 (with leap month handling)
- iching_hexagram_of_day: daily I Ching hexagram (1-64, derived from date numerology)
"""

from __future__ import annotations

import math
from datetime import date, timedelta, datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller

# -- Chinese zodiac animals --------------------------------------------------
_ZODIAC_ANIMALS = [
    "Rat", "Ox", "Tiger", "Rabbit", "Dragon", "Snake",
    "Horse", "Goat", "Monkey", "Rooster", "Dog", "Pig",
]

# -- Five elements -----------------------------------------------------------
_ELEMENTS = ["Wood", "Fire", "Earth", "Metal", "Water"]

# -- Approximate Chinese New Year dates 2015-2035 ----------------------------
# Chinese New Year falls between Jan 21 and Feb 20.  These are the actual
# dates for determining which Chinese year a given date falls in.
_CNY_DATES: dict[int, date] = {
    2015: date(2015, 2, 19),
    2016: date(2016, 2, 8),
    2017: date(2017, 1, 28),
    2018: date(2018, 2, 16),
    2019: date(2019, 2, 5),
    2020: date(2020, 1, 25),
    2021: date(2021, 2, 12),
    2022: date(2022, 2, 1),
    2023: date(2023, 1, 22),
    2024: date(2024, 2, 10),
    2025: date(2025, 1, 29),
    2026: date(2026, 2, 17),
    2027: date(2027, 2, 6),
    2028: date(2028, 1, 26),
    2029: date(2029, 2, 13),
    2030: date(2030, 2, 3),
    2031: date(2031, 1, 23),
    2032: date(2032, 2, 11),
    2033: date(2033, 1, 31),
    2034: date(2034, 2, 19),
    2035: date(2035, 2, 8),
}

# -- Feng Shui Flying Star periods ------------------------------------------
# Each period lasts 20 years.  Period 8 = 2004-2023, Period 9 = 2024-2043
_FLYING_STAR_PERIODS: list[tuple[int, int, int]] = [
    (1864, 1883, 1),
    (1884, 1903, 2),
    (1904, 1923, 3),
    (1924, 1943, 4),
    (1944, 1963, 5),
    (1964, 1983, 6),
    (1984, 2003, 7),
    (2004, 2023, 8),
    (2024, 2043, 9),
    (2044, 2063, 1),
    (2064, 2083, 2),
]

# -- Synodic month for approximate Chinese lunar month -----------------------
_SYNODIC_MONTH = 29.53059
# Reference new moon: Jan 6, 2000 18:14 UTC
_REF_NEW_MOON = datetime(2000, 1, 6, 18, 14, 0, tzinfo=timezone.utc)


def _chinese_year(d: date) -> int:
    """Return the Chinese year number for a given date.

    Uses CNY lookup table when available, otherwise approximates
    by assuming CNY falls on Feb 5.
    """
    greg_year = d.year
    cny = _CNY_DATES.get(greg_year)
    if cny is None:
        # Approximate: CNY around Feb 5
        cny = date(greg_year, 2, 5)

    if d < cny:
        return greg_year - 1
    return greg_year


def _zodiac_index(d: date) -> int:
    """Return Chinese zodiac animal index (0-11: Rat through Pig)."""
    cy = _chinese_year(d)
    return (cy - 4) % 12


def _element_index(d: date) -> int:
    """Return element index (0-4: Wood, Fire, Earth, Metal, Water)."""
    cy = _chinese_year(d)
    return ((cy - 4) % 10) // 2


def _yin_yang(d: date) -> int:
    """Return 0 for yang, 1 for yin."""
    cy = _chinese_year(d)
    return (cy - 4) % 2


def _flying_star(d: date) -> int:
    """Return the Feng Shui Flying Star number (1-9) for the current period."""
    year = d.year
    for start, end, star in _FLYING_STAR_PERIODS:
        if start <= year <= end:
            return star
    # Fallback: compute from 180-year grand cycle
    cycle_pos = (year - 1864) % 180
    return (cycle_pos // 20) % 9 + 1


def _chinese_lunar_month(d: date) -> int:
    """Return approximate Chinese lunar month (1-12).

    Uses synodic month calculation to determine which lunar month
    the date falls in.  This is an approximation; true Chinese
    calendar requires complex intercalation rules.
    """
    dt = datetime(d.year, d.month, d.day, 12, 0, 0, tzinfo=timezone.utc)
    days_since = (dt - _REF_NEW_MOON).total_seconds() / 86400.0
    # Current lunation number
    lunation = days_since / _SYNODIC_MONTH
    # Position within current lunation (0-1)
    frac = lunation - int(lunation)
    # Days into current lunar month
    day_in_month = frac * _SYNODIC_MONTH

    # Approximate which Chinese month this is:
    # Chinese month 1 starts at the new moon nearest to Feb 5
    # We use a rough mapping: Jan = 12 or 1, Feb-Dec = 1-11
    # More precisely: count new moons from the winter solstice new moon
    ref_year = d.year
    # Approximate new moon for Chinese month 1 (closest to CNY)
    cny = _CNY_DATES.get(ref_year, date(ref_year, 2, 5))
    cny_dt = datetime(cny.year, cny.month, cny.day, 12, 0, 0, tzinfo=timezone.utc)
    lunations_from_cny = (dt - cny_dt).total_seconds() / 86400.0 / _SYNODIC_MONTH
    month = int(lunations_from_cny) + 1
    # Clamp to 1-12
    month = ((month - 1) % 12) + 1
    return month


def _iching_hexagram(d: date) -> int:
    """Return I Ching hexagram of the day (1-64).

    Derived from date numerology: a deterministic hash of the date
    mapped to hexagram numbers.  Uses the traditional method of
    summing year + month + day digits and cycling through 64.
    """
    # Traditional numerological derivation
    y = d.year
    m = d.month
    day = d.day
    # Upper trigram from (year + month + day) mod 8
    upper = ((y % 100) + m + day) % 8
    # Lower trigram from (year + month * day) mod 8
    lower = ((y % 100) + m * day) % 8
    # Hexagram = upper * 8 + lower + 1 (1-indexed)
    hexagram = upper * 8 + lower + 1
    return min(max(hexagram, 1), 64)


class ChineseCalendarPuller(BasePuller):
    """Computes Chinese calendar and Feng Shui features.

    All calculations are deterministic -- no external API needed.

    Attributes:
        SOURCE_NAME: 'CHINESE_CALENDAR' in source_catalog.
    """

    SOURCE_NAME = "CHINESE_CALENDAR"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "computed://chinese-calendar",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "MED",
        "priority_rank": 93,
    }

    _SERIES = [
        "chinese_zodiac_year",
        "chinese_element",
        "chinese_yin_yang",
        "feng_shui_flying_star",
        "chinese_lunar_month",
        "iching_hexagram_of_day",
    ]

    def __init__(self, db_engine: Engine, lookback_days: int = 365) -> None:
        super().__init__(db_engine)
        self.lookback_days = lookback_days

    def _compute_day(self, d: date) -> dict[str, float]:
        """Compute all Chinese calendar features for a single date."""
        return {
            "chinese_zodiac_year": float(_zodiac_index(d)),
            "chinese_element": float(_element_index(d)),
            "chinese_yin_yang": float(_yin_yang(d)),
            "feng_shui_flying_star": float(_flying_star(d)),
            "chinese_lunar_month": float(_chinese_lunar_month(d)),
            "iching_hexagram_of_day": float(_iching_hexagram(d)),
        }

    def pull_all(self, start_date: date | None = None) -> dict[str, Any]:
        """Generate Chinese calendar features for the configured lookback window.

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
                    if self._row_exists("chinese_zodiac_year", d, conn, dedup_hours=23):
                        d += timedelta(days=1)
                        continue

                    features = self._compute_day(d)
                    for series_id, value in features.items():
                        self._insert_raw(
                            conn=conn,
                            series_id=series_id,
                            obs_date=d,
                            value=value,
                            raw_payload={"source": "chinese_calendar", "date": d.isoformat()},
                        )
                        rows_inserted += 1
                except Exception as exc:
                    errors += 1
                    log.warning(
                        "ChineseCalendarPuller error on {d}: {e}", d=d, e=str(exc)
                    )
                d += timedelta(days=1)

        status = "SUCCESS" if errors == 0 else "PARTIAL"
        log.info(
            "ChineseCalendarPuller complete -- {rows} rows, {errs} errors",
            rows=rows_inserted, errs=errors,
        )
        return {"rows_inserted": rows_inserted, "status": status}
