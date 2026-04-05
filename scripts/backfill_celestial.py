#!/usr/bin/env python3
"""GRID -- Backfill all celestial puller data from 2000-01-01 to today.

Instantiates all 5 celestial pullers and runs their pull_all() methods
with start_date=2000-01-01.  The lunar, planetary, vedic, and chinese
pullers are purely mathematical and require no API calls.  The solar
puller depends on NOAA data for some series but will still populate
solar_cycle_phase deterministically.

Usage:
    python scripts/backfill_celestial.py
    python scripts/backfill_celestial.py --start-date 2018-01-01 --end-date 2026-03-27
"""

from __future__ import annotations

import sys
import os
import time
import argparse
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

DEFAULT_START_DATE = date(2000, 1, 1)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date: {value}. Use YYYY-MM-DD."
        ) from exc


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


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="GRID celestial backfill")
    parser.add_argument(
        "--start-date",
        type=_parse_date,
        default=DEFAULT_START_DATE,
        help="Backfill start date in YYYY-MM-DD format. Default: 2000-01-01.",
    )
    parser.add_argument(
        "--end-date",
        type=_parse_date,
        default=date.today(),
        help="Backfill end date in YYYY-MM-DD format. Default: today.",
    )
    parser.add_argument(
        "--skip-solar",
        action="store_true",
        help="Skip NOAA solar pull when you only want deterministic celestial series.",
    )
    args = parser.parse_args(argv)

    start_date = args.start_date
    end_date = args.end_date
    if end_date < start_date:
        parser.error("--end-date must be on or after --start-date")

    log.info("=== GRID Celestial Backfill ===")
    log.info("Range: {start} to {end}", start=start_date, end=end_date)

    engine = create_engine(settings.DB_URL, pool_pre_ping=True)

    total_days = (end_date - start_date).days + 1
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
            result = puller.pull_all(start_date=start_date)
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
    if args.skip_solar:
        results["Solar"] = {"rows_inserted": 0, "status": "SKIPPED"}
        log.info("Solar backfill skipped by flag")
    else:
        log.info("--- Starting Solar backfill ---")
        log.info(
            "NOTE: Solar puller requires NOAA SWPC API for Kp, sunspot, wind data. "
            "Only solar_cycle_phase can be computed deterministically for all dates. "
            "API data is limited to recent days available from NOAA."
        )
        t0 = time.time()
        try:
            solar = SolarActivityPuller(db_engine=engine, lookback_days=total_days)
            result = solar.pull_all(start_date=start_date)
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
    log.info("=" * 70)
    log.info("CELESTIAL BACKFILL SUMMARY")
    log.info("=" * 70)
    log.info("Date range: {} to {} ({} days)", start_date, end_date, total_days)

    total_rows = 0
    for name, res in results.items():
        rows = res.get("rows_inserted", 0)
        status = res.get("status", "UNKNOWN")
        total_rows += rows
        err = res.get("error", "")
        line = f"  {name:12s}: {rows:>8,} rows  [{status}]"
        if err:
            line += f"  ERROR: {err[:80]}"
        log.info(line)

    log.info("  {:12s}: {:>8,} rows", "TOTAL", total_rows)

    # Show series coverage
    log.info("Series populated in raw_series:")
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
                log.info("  [{}]", src_name)
                for series_id, cnt, min_d, max_d in rows:
                    log.info("    {:30s}: {:>7,} rows  ({} to {})", series_id, cnt, min_d, max_d)

    log.info("=" * 70)
    log.info("Done.")


if __name__ == "__main__":
    main()
