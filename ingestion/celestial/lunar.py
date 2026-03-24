"""GRID — Lunar cycle data ingestion.

Computes lunar phase features from astronomical ephemeris using pure math
(no external API required).  All calculations use the synodic month algorithm
relative to the J2000 reference new moon.

Features generated:
- lunar_phase: 0.0 (new moon) to 1.0 (full moon) and back
- lunar_illumination: percentage of moon illuminated (0-100)
- days_to_new_moon: calendar days until next new moon
- days_to_full_moon: calendar days until next full moon
- lunar_eclipse_proximity: days to nearest lunar eclipse (0 = eclipse day)
- solar_eclipse_proximity: days to nearest solar eclipse
"""

from __future__ import annotations

import math
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller

# ── Astronomical constants ──────────────────────────────────────────
SYNODIC_MONTH = 29.53059  # days
# Reference new moon: 2000-01-06 18:14 UTC (J2000 epoch new moon)
_REF_NEW_MOON = datetime(2000, 1, 6, 18, 14, 0, tzinfo=timezone.utc)

# Known lunar eclipses 2020-2030 (date of maximum eclipse)
_LUNAR_ECLIPSES: list[date] = [
    date(2020, 1, 10), date(2020, 6, 5), date(2020, 7, 5),
    date(2020, 11, 30),
    date(2021, 5, 26), date(2021, 11, 19),
    date(2022, 5, 16), date(2022, 11, 8),
    date(2023, 5, 5), date(2023, 10, 28),
    date(2024, 3, 25), date(2024, 9, 18),
    date(2025, 3, 14), date(2025, 9, 7),
    date(2026, 3, 3), date(2026, 8, 28),
    date(2027, 2, 20), date(2027, 7, 18), date(2027, 8, 17),
    date(2028, 1, 12), date(2028, 7, 6), date(2028, 12, 31),
    date(2029, 6, 26), date(2029, 12, 20),
    date(2030, 6, 15), date(2030, 12, 9),
]

# Known solar eclipses 2020-2030 (date of maximum eclipse)
_SOLAR_ECLIPSES: list[date] = [
    date(2020, 6, 21), date(2020, 12, 14),
    date(2021, 6, 10), date(2021, 12, 4),
    date(2022, 4, 30), date(2022, 10, 25),
    date(2023, 4, 20), date(2023, 10, 14),
    date(2024, 4, 8), date(2024, 10, 2),
    date(2025, 3, 29), date(2025, 9, 21),
    date(2026, 2, 17), date(2026, 8, 12),
    date(2027, 2, 6), date(2027, 8, 2),
    date(2028, 1, 26), date(2028, 7, 22),
    date(2029, 1, 14), date(2029, 6, 12), date(2029, 7, 11),
    date(2029, 12, 5),
    date(2030, 6, 1), date(2030, 11, 25),
]


def _lunar_phase(d: date) -> float:
    """Return lunar phase as fraction of synodic month (0 = new, 0.5 = full)."""
    dt = datetime(d.year, d.month, d.day, 12, 0, 0, tzinfo=timezone.utc)
    days_since_ref = (dt - _REF_NEW_MOON).total_seconds() / 86400.0
    return (days_since_ref % SYNODIC_MONTH) / SYNODIC_MONTH


def _illumination(phase: float) -> float:
    """Return percentage illumination (0-100) from phase fraction."""
    return (1.0 - math.cos(phase * 2.0 * math.pi)) / 2.0 * 100.0


def _days_to_phase(d: date, target_phase: float) -> float:
    """Return calendar days from *d* to the next occurrence of *target_phase*.

    Parameters:
        d: Current date.
        target_phase: Target phase fraction (0.0 = new, 0.5 = full).
    """
    current = _lunar_phase(d)
    diff = target_phase - current
    if diff < 0:
        diff += 1.0
    return diff * SYNODIC_MONTH


def _nearest_eclipse(d: date, eclipse_list: list[date]) -> int:
    """Return absolute days to the nearest eclipse in *eclipse_list*."""
    if not eclipse_list:
        return 9999
    return min(abs((e - d).days) for e in eclipse_list)


class LunarCyclePuller(BasePuller):
    """Computes lunar cycle features from astronomical ephemeris.

    All calculations are deterministic (pure math) — no external API needed.
    Features are generated for each calendar day in the requested range.

    Attributes:
        SOURCE_NAME: 'LUNAR_EPHEMERIS' in source_catalog.
    """

    SOURCE_NAME = "LUNAR_EPHEMERIS"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "computed://ephemeris",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 90,
    }

    # Series IDs matching feature_registry names
    _SERIES = [
        "lunar_phase",
        "lunar_illumination",
        "days_to_new_moon",
        "days_to_full_moon",
        "lunar_eclipse_proximity",
        "solar_eclipse_proximity",
    ]

    def __init__(self, db_engine: Engine, lookback_days: int = 365) -> None:
        super().__init__(db_engine)
        self.lookback_days = lookback_days

    def _compute_day(self, d: date) -> dict[str, float]:
        """Compute all lunar features for a single date."""
        phase = _lunar_phase(d)
        return {
            "lunar_phase": round(phase, 6),
            "lunar_illumination": round(_illumination(phase), 4),
            "days_to_new_moon": round(_days_to_phase(d, 0.0), 2),
            "days_to_full_moon": round(_days_to_phase(d, 0.5), 2),
            "lunar_eclipse_proximity": _nearest_eclipse(d, _LUNAR_ECLIPSES),
            "solar_eclipse_proximity": _nearest_eclipse(d, _SOLAR_ECLIPSES),
        }

    def pull_all(self, start_date: date | None = None) -> dict[str, Any]:
        """Generate lunar features for the configured lookback window.

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
                    if self._row_exists("lunar_phase", d, conn, dedup_hours=23):
                        d += timedelta(days=1)
                        continue

                    features = self._compute_day(d)
                    for series_id, value in features.items():
                        self._insert_raw(
                            conn=conn,
                            series_id=series_id,
                            obs_date=d,
                            value=value,
                            raw_payload={"source": "ephemeris", "date": d.isoformat()},
                        )
                        rows_inserted += 1
                except Exception as exc:
                    errors += 1
                    log.warning(
                        "LunarCyclePuller error on {d}: {e}", d=d, e=str(exc)
                    )
                d += timedelta(days=1)

        status = "SUCCESS" if errors == 0 else "PARTIAL"
        log.info(
            "LunarCyclePuller complete — {rows} rows, {errs} errors",
            rows=rows_inserted, errs=errors,
        )
        return {"rows_inserted": rows_inserted, "status": status}
