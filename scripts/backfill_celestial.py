#!/usr/bin/env python3
"""GRID -- Backfill all celestial puller data from 2000-01-01 to today.

Instantiates all 5 celestial pullers and runs their pull_all() methods
with start_date=2000-01-01.  The lunar, planetary, vedic, and chinese
pullers are purely mathematical and require no API calls.  The solar
puller depends on NOAA data for some series but will still populate
solar_cycle_phase deterministically.

Usage:
    cd /data/grid_v4/grid_repo/grid
    python scripts/backfill_celestial.py
"""

from __future__ import annotations

import sys
import os
import time
from datetime import date, timedelta

# Ensure the grid package root is on sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loguru import logger as log
from sqlalchemy import create_engine, text

from config import settings
from ingestion.celestial.planetary import PlanetaryAspectPuller
from ingestion.celestial.lunar import LunarCyclePuller
from ingestion.celestial.solar import SolarActivityPuller
from ingestion.celestial.vedic import VedicAstroPuller
from ingestion.celestial.chinese import ChineseCalendarPuller

START_DATE = date(2000, 1, 1)
END_DATE = date(2026, 3, 26)


def count_rows(engine, source_name: str) -> int:
    """Count raw_series rows for a given source."""
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT COUNT(*) FROM raw_series rs "
                "JOIN source_catalog sc ON rs.source_id = sc.id "
                "WHERE sc.name = :name"
            ),
            {"name": source_name},
        ).fetchone()
        return row[0] if row else 0


def main() -> None:
    log.info("=== GRID Celestial Backfill ===")
    log.info("Range: {start} to {end}", start=START_DATE, end=END_DATE)

    engine = create_engine(settings.DB_URL, pool_pre_ping=True)

    total_days = (END_DATE - START_DATE).days + 1
    log.info("Total days to cover: {n}", n=total_days)

    # --- 1. Mathematical pullers (no API needed) ---
    math_pullers = [
        ("Planetary", PlanetaryAspectPuller),
        ("Lunar", LunarCyclePuller),
        ("Vedic", VedicAstroPuller),
        ("Chinese", ChineseCalendarPuller),
    ]

    results = {}

    for name, PullerClass in math_pullers:
        log.info("--- Starting {name} backfill ---", name=name)
        t0 = time.time()
        try:
            puller = PullerClass(db_engine=engine, lookback_days=total_days)
            result = puller.pull_all(start_date=START_DATE)
            elapsed = time.time() - t0
            results[name] = result
            log.info(
                "{name} complete: {rows} rows in {t:.1f}s — status={status}",
                name=name,
                rows=result["rows_inserted"],
                t=elapsed,
                status=result["status"],
            )
        except Exception as exc:
            elapsed = time.time() - t0
            results[name] = {"rows_inserted": 0, "status": "FAILED", "error": str(exc)}
            log.error(
                "{name} FAILED after {t:.1f}s: {e}",
                name=name,
                t=elapsed,
                e=str(exc),
            )

    # --- 2. Solar puller (needs NOAA API for some series) ---
    log.info("--- Starting Solar backfill ---")
    log.info(
        "NOTE: Solar puller requires NOAA SWPC API for Kp, sunspot, wind data. "
        "Only solar_cycle_phase can be computed deterministically for all dates. "
        "API data is limited to recent days available from NOAA."
    )
    t0 = time.time()
    try:
        solar = SolarActivityPuller(db_engine=engine, lookback_days=total_days)
        result = solar.pull_all(start_date=START_DATE)
        elapsed = time.time() - t0
        results["Solar"] = result
        log.info(
            "Solar complete: {rows} rows in {t:.1f}s — status={status}",
            rows=result["rows_inserted"],
            t=elapsed,
            status=result["status"],
        )
    except Exception as exc:
        elapsed = time.time() - t0
        results["Solar"] = {"rows_inserted": 0, "status": "FAILED", "error": str(exc)}
        log.error("Solar FAILED after {t:.1f}s: {e}", t=elapsed, e=str(exc))

    # --- Summary ---
    print("\n" + "=" * 70)
    print("CELESTIAL BACKFILL SUMMARY")
    print("=" * 70)
    print(f"Date range: {START_DATE} to {END_DATE} ({total_days} days)")
    print()

    total_rows = 0
    for name, res in results.items():
        rows = res.get("rows_inserted", 0)
        status = res.get("status", "UNKNOWN")
        total_rows += rows
        err = res.get("error", "")
        line = f"  {name:12s}: {rows:>8,} rows  [{status}]"
        if err:
            line += f"  ERROR: {err[:80]}"
        print(line)

    print(f"\n  {'TOTAL':12s}: {total_rows:>8,} rows")
    print()

    # Show series coverage
    print("Series populated in raw_series:")
    source_names = [
        "PLANETARY_EPHEMERIS",
        "LUNAR_EPHEMERIS",
        "VEDIC_JYOTISH",
        "CHINESE_CALENDAR",
        "NOAA_SWPC",
    ]
    with engine.connect() as conn:
        for src_name in source_names:
            rows = conn.execute(
                text(
                    "SELECT rs.series_id, COUNT(*), MIN(rs.obs_date), MAX(rs.obs_date) "
                    "FROM raw_series rs "
                    "JOIN source_catalog sc ON rs.source_id = sc.id "
                    "WHERE sc.name = :name "
                    "GROUP BY rs.series_id ORDER BY rs.series_id"
                ),
                {"name": src_name},
            ).fetchall()
            if rows:
                print(f"\n  [{src_name}]")
                for series_id, cnt, min_d, max_d in rows:
                    print(f"    {series_id:30s}: {cnt:>7,} rows  ({min_d} to {max_d})")

    print("\n" + "=" * 70)
    print("Done.")


if __name__ == "__main__":
    main()
