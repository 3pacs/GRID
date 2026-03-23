#!/usr/bin/env python3
"""GRID v4 — Full Data Ingest Pipeline"""

import duckdb, json, os, sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))
from sources_expanded import ALL_FETCHERS, fetch_all_fred

DB_PATH = os.environ.get("GRID_DUCKDB_PATH", os.path.expanduser("~/grid_v4/data/grid.duckdb"))
LOG_DIR = os.environ.get("GRID_LOG_DIR", os.path.expanduser("~/grid_v4/logs"))
Path(LOG_DIR).mkdir(parents=True, exist_ok=True)

def log(msg):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(f"{LOG_DIR}/ingest.log", "a") as f:
        f.write(line + "\n")

def run():
    log("=" * 50)
    log("GRID INGEST PIPELINE — START")
    log("=" * 50)

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
            log(f"  {name}: {status} ({count})")
            if count > 0: success += 1
        except Exception as e:
            db.execute("INSERT INTO ingest_log VALUES (?,?,?,?,?)",
                [name, datetime.utcnow(), "ERROR", 0, str(e)[:200]])
            log(f"  {name}: ERROR — {str(e)[:80]}")

    total += 1
    try:
        fred_count = fetch_all_fred(db)
        log(f"  fred_all: OK ({fred_count}/20 series)")
        if fred_count > 0: success += 1
    except Exception as e:
        log(f"  fred_all: ERROR — {str(e)[:80]}")

    db.close()
    log(f"\nPIPELINE COMPLETE: {success}/{total} sources")
    log("=" * 50)

if __name__ == "__main__":
    run()
