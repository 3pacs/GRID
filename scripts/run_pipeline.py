#!/usr/bin/env python3
"""GRID v4 — Full Data Ingest Pipeline"""

import duckdb, json, os, sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))
from sources_expanded import ALL_FETCHERS, fetch_all_fred
from loguru import logger as log

DB_PATH = os.environ.get("GRID_DUCKDB_PATH", os.path.expanduser("~/grid_v4/data/grid.duckdb"))
LOG_DIR = os.environ.get("GRID_LOG_DIR", os.path.expanduser("~/grid_v4/logs"))
Path(LOG_DIR).mkdir(parents=True, exist_ok=True)

# Add file sink for pipeline log
log.add(f"{LOG_DIR}/ingest.log", rotation="10 MB", retention="30 days")

def run():
    log.info("=" * 50)
    log.info("GRID INGEST PIPELINE — START")
    log.info("=" * 50)

    db = duckdb.connect(DB_PATH)
    db.execute("""CREATE TABLE IF NOT EXISTS raw_ingest (
        source VARCHAR, tier INTEGER, fetched_at TIMESTAMP,
        data_date DATE, payload JSON)""")
    db.execute("""CREATE TABLE IF NOT EXISTS ingest_log (
        source VARCHAR, fetched_at TIMESTAMP, status VARCHAR,
        records INTEGER, error VARCHAR)""")

    success = 0
    total = 0

    for name, func in ALL_FETCHERS:
        total += 1
        try:
            count = func(db)
            status = "OK" if count > 0 else "EMPTY"
            db.execute("INSERT INTO ingest_log VALUES (?,?,?,?,?)",
                [name, datetime.utcnow(), status, count, None])
            log.info("  {}: {} ({})", name, status, count)
            if count > 0: success += 1
        except Exception as e:
            db.execute("INSERT INTO ingest_log VALUES (?,?,?,?,?)",
                [name, datetime.utcnow(), "ERROR", 0, str(e)[:200]])
            log.error("  {}: ERROR — {}", name, str(e)[:80])

    total += 1
    try:
        fred_count = fetch_all_fred(db)
        log.info("  fred_all: OK ({}/20 series)", fred_count)
        if fred_count > 0: success += 1
    except Exception as e:
        log.error("  fred_all: ERROR — {}", str(e)[:80])

    db.close()
    log.info("PIPELINE COMPLETE: {}/{} sources", success, total)
    log.info("=" * 50)

if __name__ == "__main__":
    run()
