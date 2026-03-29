#!/usr/bin/env python3
"""GRID -- Backfill ephemeris data for AstroGrid correlation engine.

Computes and stores daily ephemeris data from 2000-01-01 to 2026-03-26:
  - Geocentric longitude for each planet (Mercury through Saturn)
  - Daily aspect count (hard aspects: conjunction, square, opposition)
  - Lunar phase (0=new, 0.5=full) and illumination percentage
  - Nakshatra index (0-26)
  - Retrograde status for each planet (0 or 1)

All calculations are deterministic (simplified Keplerian elements).
No external APIs required.

Stores in raw_series with series_ids prefixed by 'ephemeris.'.
Uses batch inserts (1000 rows) for efficiency and skips existing dates.

Usage:
    cd /data/grid_v4/grid_repo/grid
    python scripts/backfill_celestial_ephemeris.py
"""

from __future__ import annotations

import json
import math
import sys
import os
import time
import argparse
from datetime import date, datetime, timedelta, timezone
from typing import Any

# Ensure the grid package root is on sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loguru import logger as log
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config import settings

# ============================================================================
# Ephemeris computation engine (standalone)
# ============================================================================

# J2000 epoch reference
_J2000 = date(2000, 1, 1)

# Simplified orbital elements: (L0_deg at J2000, rate_deg_per_day)
# For geocentric longitude we compute heliocentric then subtract Earth
_ORBITAL_ELEMENTS: dict[str, tuple[float, float]] = {
    "mercury": (252.251, 4.09233),
    "venus":   (181.980, 1.60213),
    "earth":   (100.464, 0.98560),
    "mars":    (355.453, 0.52403),
    "jupiter": ( 34.351, 0.08309),
    "saturn":  ( 50.077, 0.03346),
}

# Planets we compute longitudes for (not Earth)
PLANETS = ["mercury", "venus", "mars", "jupiter", "saturn"]

# Synodic periods for retrograde detection (days)
# When geocentric longitude rate goes negative, planet is retrograde
_SYNODIC_PERIODS: dict[str, float] = {
    "mercury": 115.88,
    "venus":   583.9,
    "mars":    779.9,
    "jupiter": 398.9,
    "saturn":  378.1,
}

# Lunar constants
_SYNODIC_MONTH = 29.53059
_REF_NEW_MOON = datetime(2000, 1, 6, 18, 14, 0, tzinfo=timezone.utc)

# Moon sidereal motion
_MOON_L0 = 218.3165  # degrees at J2000
_MOON_RATE = 13.17640  # degrees per day

# Nakshatra span
_NAKSHATRA_SPAN = 360.0 / 27.0  # 13.3333... degrees

# Hard aspect targets and orb
_HARD_ASPECT_TARGETS = [0.0, 90.0, 180.0]
_SOFT_ASPECT_TARGETS = [60.0, 120.0]
_ORB = 8.0  # degrees


def _helio_longitude(planet: str, d: date) -> float:
    """Heliocentric ecliptic longitude in degrees."""
    L0, rate = _ORBITAL_ELEMENTS[planet]
    days = (d - _J2000).days
    return (L0 + rate * days) % 360.0


def _geo_longitude(planet: str, d: date) -> float:
    """Approximate geocentric longitude (helio minus Earth)."""
    if planet == "earth":
        return 0.0
    return (_helio_longitude(planet, d) - _helio_longitude("earth", d)) % 360.0


def _angular_separation(lon1: float, lon2: float) -> float:
    """Angular separation in degrees (0-180)."""
    diff = abs(lon1 - lon2) % 360.0
    return diff if diff <= 180.0 else 360.0 - diff


def _is_retrograde(planet: str, d: date) -> bool:
    """Check if planet appears retrograde on date d.

    Retrograde = geocentric longitude is decreasing (rate < 0).
    We approximate by comparing longitude on d-1 and d+1.
    """
    lon_before = _geo_longitude(planet, d - timedelta(days=1))
    lon_after = _geo_longitude(planet, d + timedelta(days=1))
    # Handle wrap-around at 0/360
    diff = lon_after - lon_before
    if diff > 180.0:
        diff -= 360.0
    elif diff < -180.0:
        diff += 360.0
    return diff < 0.0


def _hard_aspect_count(d: date) -> int:
    """Count hard aspects (conjunction, square, opposition) among all planet pairs."""
    lons = {p: _geo_longitude(p, d) for p in PLANETS}
    count = 0
    for i, p1 in enumerate(PLANETS):
        for p2 in PLANETS[i + 1:]:
            sep = _angular_separation(lons[p1], lons[p2])
            for target in _HARD_ASPECT_TARGETS:
                if abs(sep - target) < _ORB:
                    count += 1
                    break  # count each pair at most once
    return count


def _soft_aspect_count(d: date) -> int:
    """Count soft aspects (sextile, trine) among all planet pairs."""
    lons = {p: _geo_longitude(p, d) for p in PLANETS}
    count = 0
    for i, p1 in enumerate(PLANETS):
        for p2 in PLANETS[i + 1:]:
            sep = _angular_separation(lons[p1], lons[p2])
            for target in _SOFT_ASPECT_TARGETS:
                if abs(sep - target) < _ORB:
                    count += 1
                    break
    return count


def _lunar_phase(d: date) -> float:
    """Lunar phase as fraction of synodic month (0=new, 0.5=full)."""
    dt = datetime(d.year, d.month, d.day, 12, 0, 0, tzinfo=timezone.utc)
    days_since = (dt - _REF_NEW_MOON).total_seconds() / 86400.0
    return (days_since % _SYNODIC_MONTH) / _SYNODIC_MONTH


def _lunar_illumination(phase: float) -> float:
    """Percentage illumination (0-100) from phase fraction."""
    return (1.0 - math.cos(phase * 2.0 * math.pi)) / 2.0 * 100.0


def _lunar_age_days(phase: float) -> float:
    """Lunar age in days within the synodic month."""
    return phase * _SYNODIC_MONTH


def _moon_sidereal_longitude(d: date) -> float:
    """Moon's sidereal longitude in degrees (0-360)."""
    days = (d - _J2000).days
    return (_MOON_L0 + _MOON_RATE * days) % 360.0


def _nakshatra_index(d: date) -> int:
    """Nakshatra index (0-26) from Moon's sidereal longitude."""
    lon = _moon_sidereal_longitude(d)
    return int(lon / _NAKSHATRA_SPAN) % 27


def _nakshatra_pada(d: date) -> int:
    """Nakshatra quarter index (1-4)."""
    lon = _moon_sidereal_longitude(d)
    within_nakshatra = lon % _NAKSHATRA_SPAN
    return int(within_nakshatra / (_NAKSHATRA_SPAN / 4.0)) + 1


def _tithi_index(phase: float) -> int:
    """Tithi index (0-29) from lunar phase fraction."""
    return int((phase * 30.0) % 30.0)


def _phase_bucket(phase: float) -> int:
    """Coarse eight-phase lunar bucket (0-7)."""
    return int((phase * 8.0) % 8.0)


def compute_ephemeris_day(d: date) -> dict[str, float]:
    """Compute all ephemeris features for a single date.

    Returns dict with series_id -> value mappings.
    """
    features: dict[str, float] = {}

    # Planetary longitudes
    for planet in PLANETS:
        lon = _geo_longitude(planet, d)
        features[f"ephemeris.{planet}.longitude"] = round(lon, 4)

    # Retrograde status
    for planet in PLANETS:
        features[f"ephemeris.{planet}_retrograde"] = 1.0 if _is_retrograde(planet, d) else 0.0

    # Aspect count
    hard_count = _hard_aspect_count(d)
    soft_count = _soft_aspect_count(d)
    features["ephemeris.aspect_count"] = float(hard_count)
    features["ephemeris.hard_aspect_count"] = float(hard_count)
    features["ephemeris.soft_aspect_count"] = float(soft_count)

    # Lunar
    phase = _lunar_phase(d)
    features["ephemeris.lunar_phase"] = round(phase, 6)
    features["ephemeris.lunar_illumination"] = round(_lunar_illumination(phase), 4)
    features["ephemeris.lunar_age_days"] = round(_lunar_age_days(phase), 4)
    features["ephemeris.tithi_index"] = float(_tithi_index(phase))
    features["ephemeris.phase_bucket"] = float(_phase_bucket(phase))

    # Nakshatra
    features["ephemeris.nakshatra_index"] = float(_nakshatra_index(d))
    features["ephemeris.nakshatra_pada"] = float(_nakshatra_pada(d))

    return features


# ============================================================================
# Database operations
# ============================================================================

# Source name for ephemeris data
EPHEMERIS_SOURCE_NAME = "EPHEMERIS_ENGINE"
EPHEMERIS_SOURCE_CONFIG = {
    "base_url": "computed://ephemeris-engine",
    "cost_tier": "FREE",
    "latency_class": "EOD",
    "pit_available": True,
    "revision_behavior": "NEVER",
    "trust_score": "HIGH",
    "priority_rank": 89,
}

DEFAULT_START_DATE = date(2000, 1, 1)
DEFAULT_END_DATE = date(2026, 3, 26)
DEFAULT_BATCH_SIZE = 1000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", default=DEFAULT_START_DATE.isoformat(), help="Backfill start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", default=DEFAULT_END_DATE.isoformat(), help="Backfill end date (YYYY-MM-DD)")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Insert batch size")
    return parser.parse_args()


def resolve_source_id(engine: Engine) -> int:
    """Look up or create the EPHEMERIS_ENGINE source_catalog entry."""
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT id FROM source_catalog WHERE name = :name"),
            {"name": EPHEMERIS_SOURCE_NAME},
        ).fetchone()
    if row is not None:
        return row[0]

    log.info("Auto-creating source_catalog entry for {s}", s=EPHEMERIS_SOURCE_NAME)
    with engine.begin() as conn:
        result = conn.execute(
            text(
                "INSERT INTO source_catalog "
                "(name, base_url, cost_tier, latency_class, pit_available, "
                "revision_behavior, trust_score, priority_rank, active) "
                "VALUES (:name, :url, :cost, :latency, :pit, :rev, :trust, :rank, TRUE) "
                "ON CONFLICT (name) DO NOTHING "
                "RETURNING id"
            ),
            {
                "name": EPHEMERIS_SOURCE_NAME,
                "url": EPHEMERIS_SOURCE_CONFIG["base_url"],
                "cost": EPHEMERIS_SOURCE_CONFIG["cost_tier"],
                "latency": EPHEMERIS_SOURCE_CONFIG["latency_class"],
                "pit": EPHEMERIS_SOURCE_CONFIG["pit_available"],
                "rev": EPHEMERIS_SOURCE_CONFIG["revision_behavior"],
                "trust": EPHEMERIS_SOURCE_CONFIG["trust_score"],
                "rank": EPHEMERIS_SOURCE_CONFIG["priority_rank"],
            },
        )
        new_row = result.fetchone()
        if new_row:
            return new_row[0]

    # ON CONFLICT hit -- re-fetch
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT id FROM source_catalog WHERE name = :name"),
            {"name": EPHEMERIS_SOURCE_NAME},
        ).fetchone()
    return row[0]


def get_existing_dates(engine: Engine, source_id: int) -> set[date]:
    """Fetch all dates already stored for ANY ephemeris series.

    We check a single representative series to determine which dates
    are already backfilled (all series are written together per date).
    """
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT DISTINCT obs_date FROM raw_series "
                "WHERE series_id = 'ephemeris.mars.longitude' "
                "AND source_id = :src AND pull_status = 'SUCCESS'"
            ),
            {"src": source_id},
        ).fetchall()
    return {r[0] for r in rows}


def flush_batch(engine: Engine, batch: list[dict], source_id: int) -> int:
    """Insert a batch of rows into raw_series. Returns count inserted."""
    if not batch:
        return 0
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO raw_series "
                "(series_id, source_id, obs_date, value, raw_payload, pull_status) "
                "VALUES (:sid, :src, :od, :val, :payload, 'SUCCESS')"
            ),
            batch,
        )
    return len(batch)


def main() -> None:
    args = parse_args()
    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)
    batch_size = max(1, int(args.batch_size))

    log.info("=== GRID Ephemeris Backfill ===")
    log.info("Range: {start} to {end}", start=start_date, end=end_date)

    engine = create_engine(settings.DB_URL, pool_pre_ping=True)
    source_id = resolve_source_id(engine)
    log.info("Source ID: {sid}", sid=source_id)

    # Get existing dates to skip
    existing = get_existing_dates(engine, source_id)
    log.info("Found {n} dates already in database — will skip", n=len(existing))

    total_days = (end_date - start_date).days + 1
    log.info("Total days in range: {n}", n=total_days)

    # Determine the series we will produce (compute one day to get keys)
    sample = compute_ephemeris_day(start_date)
    series_ids = sorted(sample.keys())
    log.info("Series to compute ({n}):", n=len(series_ids))
    for sid in series_ids:
        log.info("  - {s}", s=sid)

    # Main loop with batch inserts
    batch: list[dict] = []
    rows_inserted = 0
    days_computed = 0
    days_skipped = 0
    errors = 0
    t0 = time.time()

    d = start_date
    while d <= end_date:
        # Skip existing
        if d in existing:
            days_skipped += 1
            d += timedelta(days=1)
            continue

        try:
            features = compute_ephemeris_day(d)
            payload_str = json.dumps({"source": "ephemeris_engine", "date": d.isoformat()})

            for series_id, value in features.items():
                batch.append({
                    "sid": series_id,
                    "src": source_id,
                    "od": d,
                    "val": value,
                    "payload": payload_str,
                })

            days_computed += 1

            # Flush batch when full
            if len(batch) >= batch_size:
                rows_inserted += flush_batch(engine, batch, source_id)
                batch.clear()

        except Exception as exc:
            errors += 1
            if errors <= 10:
                log.warning("Error on {d}: {e}", d=d, e=str(exc))

        # Progress reporting
        day_num = (d - start_date).days + 1
        if day_num % 1000 == 0:
            elapsed = time.time() - t0
            pct = day_num / total_days * 100
            rate = day_num / elapsed if elapsed > 0 else 0
            log.info(
                "Progress: {n}/{total} days ({pct:.1f}%) — "
                "{rate:.0f} days/sec — {rows:,} rows inserted",
                n=day_num, total=total_days, pct=pct,
                rate=rate, rows=rows_inserted,
            )

        d += timedelta(days=1)

    # Flush remaining
    if batch:
        rows_inserted += flush_batch(engine, batch, source_id)
        batch.clear()

    elapsed = time.time() - t0

    # --- Summary ---
    print("\n" + "=" * 70)
    print("EPHEMERIS BACKFILL SUMMARY")
    print("=" * 70)
    print(f"Date range:     {start_date} to {end_date} ({total_days} days)")
    print(f"Days computed:  {days_computed:,}")
    print(f"Days skipped:   {days_skipped:,} (already in DB)")
    print(f"Rows inserted:  {rows_inserted:,}")
    print(f"Errors:         {errors}")
    print(f"Elapsed:        {elapsed:.1f}s")
    if days_computed > 0:
        print(f"Rate:           {days_computed / elapsed:.0f} days/sec")
    print()

    # Show series details
    print(f"Series populated ({len(series_ids)}):")
    with engine.connect() as conn:
        for sid in series_ids:
            row = conn.execute(
                text(
                    "SELECT COUNT(*), MIN(obs_date), MAX(obs_date) "
                    "FROM raw_series "
                    "WHERE series_id = :sid AND source_id = :src"
                ),
                {"sid": sid, "src": source_id},
            ).fetchone()
            if row and row[0] > 0:
                print(f"  {sid:40s}: {row[0]:>7,} rows  ({row[1]} to {row[2]})")
            else:
                print(f"  {sid:40s}: 0 rows")

    print("\n" + "=" * 70)
    print("Done.")


if __name__ == "__main__":
    main()
