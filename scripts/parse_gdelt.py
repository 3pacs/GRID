#!/usr/bin/env python3
"""GRID — Parse GDELT daily event CSV zips into PostgreSQL.

Reads pre-downloaded GDELT event CSVs from /data/grid/bulk/gdelt/,
loads into gdelt_events table, then builds gdelt_daily_summary with
conflict/cooperation counts, Goldstein scale, tone, and per-country event counts.

Pushes 12 features to resolved_series.

Run: python3 parse_gdelt.py
"""

import os
import sys
import csv
import zipfile
import io
from datetime import datetime
from pathlib import Path

import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings
from loguru import logger as log

BULK_DIR = Path("/data/grid/bulk/gdelt")

# GDELT export CSV column indices (v2)
COL_GLOBALEVENTID = 0
COL_DAY = 1
COL_ACTOR1CODE = 5
COL_ACTOR1COUNTRYCODE = 7
COL_ACTOR2CODE = 15
COL_ACTOR2COUNTRYCODE = 17
COL_ISROOTEVENT = 25
COL_EVENTCODE = 26
COL_EVENTBASECODE = 27
COL_EVENTROOTCODE = 28
COL_QUADCLASS = 29
COL_GOLDSTEINSCALE = 30
COL_NUMMENTIONS = 31
COL_NUMSOURCES = 32
COL_NUMARTICLES = 33
COL_AVGTONE = 34

# Countries to track individually
TRACKED_COUNTRIES = ["US", "CN", "RU", "IR", "TW", "UA"]

FEATURES = {
    "gdelt_total_events": ("sentiment", "GDELT Total Daily Events"),
    "gdelt_conflict_count": ("sentiment", "GDELT Daily Conflict Events (QuadClass 3+4)"),
    "gdelt_cooperation_count": ("sentiment", "GDELT Daily Cooperation Events (QuadClass 1+2)"),
    "gdelt_avg_goldstein": ("sentiment", "GDELT Daily Average Goldstein Scale"),
    "gdelt_avg_tone": ("sentiment", "GDELT Daily Average Tone"),
    "gdelt_num_mentions": ("sentiment", "GDELT Daily Total Mentions"),
}
for cc in TRACKED_COUNTRIES:
    FEATURES[f"gdelt_events_{cc.lower()}"] = ("sentiment", f"GDELT Daily Events Involving {cc}")


def connect():
    return psycopg2.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        dbname=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
    )


def create_tables(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS gdelt_events (
            global_event_id    BIGINT PRIMARY KEY,
            event_date         DATE NOT NULL,
            actor1_country     TEXT,
            actor2_country     TEXT,
            event_root_code    TEXT,
            quad_class         INTEGER,
            goldstein_scale    DOUBLE PRECISION,
            num_mentions       INTEGER,
            num_sources        INTEGER,
            num_articles       INTEGER,
            avg_tone           DOUBLE PRECISION
        );
        CREATE INDEX IF NOT EXISTS idx_gdelt_date ON gdelt_events (event_date);
        CREATE INDEX IF NOT EXISTS idx_gdelt_quad ON gdelt_events (quad_class);
        CREATE INDEX IF NOT EXISTS idx_gdelt_actor1 ON gdelt_events (actor1_country);

        CREATE TABLE IF NOT EXISTS gdelt_daily_summary (
            summary_date         DATE PRIMARY KEY,
            total_events         INTEGER,
            conflict_count       INTEGER,
            cooperation_count    INTEGER,
            avg_goldstein        DOUBLE PRECISION,
            avg_tone             DOUBLE PRECISION,
            total_mentions       BIGINT,
            country_events       JSONB
        );
    """)


def safe_int(val, default=0):
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def safe_float(val, default=0.0):
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def parse_date(date_str):
    """Parse GDELT date (YYYYMMDD) to ISO format."""
    date_str = str(date_str).strip()
    if len(date_str) == 8:
        return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    return None


def parse_csv_file(filepath, cur):
    """Parse a single GDELT CSV (may be inside a zip or raw CSV)."""
    rows_loaded = 0
    batch = []

    def process_lines(lines):
        nonlocal rows_loaded, batch
        reader = csv.reader(lines, delimiter="\t")
        for row in reader:
            if len(row) < 35:
                continue
            try:
                event_id = int(row[COL_GLOBALEVENTID])
                event_date = parse_date(row[COL_DAY])
                if not event_date:
                    continue
                batch.append((
                    event_id,
                    event_date,
                    row[COL_ACTOR1COUNTRYCODE][:3] if row[COL_ACTOR1COUNTRYCODE] else None,
                    row[COL_ACTOR2COUNTRYCODE][:3] if row[COL_ACTOR2COUNTRYCODE] else None,
                    row[COL_EVENTROOTCODE][:4] if row[COL_EVENTROOTCODE] else None,
                    safe_int(row[COL_QUADCLASS]),
                    safe_float(row[COL_GOLDSTEINSCALE]),
                    safe_int(row[COL_NUMMENTIONS]),
                    safe_int(row[COL_NUMSOURCES]),
                    safe_int(row[COL_NUMARTICLES]),
                    safe_float(row[COL_AVGTONE]),
                ))
            except (ValueError, IndexError):
                continue

            if len(batch) >= 10000:
                _flush_batch(cur, batch)
                rows_loaded += len(batch)
                batch = []

    filepath = Path(filepath)
    if filepath.suffix == ".zip":
        with zipfile.ZipFile(filepath) as zf:
            for name in zf.namelist():
                if name.endswith(".CSV") or name.endswith(".csv") or name.endswith(".export.CSV"):
                    with zf.open(name) as f:
                        lines = io.TextIOWrapper(f, encoding="utf-8", errors="replace")
                        process_lines(lines)
    elif filepath.suffix in (".csv", ".CSV"):
        with open(filepath, encoding="utf-8", errors="replace") as f:
            process_lines(f)

    if batch:
        _flush_batch(cur, batch)
        rows_loaded += len(batch)

    return rows_loaded


def _flush_batch(cur, batch):
    args = ",".join(
        cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", row).decode() for row in batch
    )
    cur.execute(
        "INSERT INTO gdelt_events (global_event_id,event_date,actor1_country,actor2_country,"
        "event_root_code,quad_class,goldstein_scale,num_mentions,num_sources,num_articles,avg_tone) "
        "VALUES " + args + " ON CONFLICT (global_event_id) DO NOTHING"
    )


def build_daily_summaries(cur):
    """Aggregate gdelt_events into gdelt_daily_summary."""
    log.info("Building daily summaries...")
    cur.execute("""
        INSERT INTO gdelt_daily_summary (summary_date, total_events, conflict_count,
            cooperation_count, avg_goldstein, avg_tone, total_mentions, country_events)
        SELECT
            event_date,
            COUNT(*),
            COUNT(*) FILTER (WHERE quad_class IN (3, 4)),
            COUNT(*) FILTER (WHERE quad_class IN (1, 2)),
            AVG(goldstein_scale),
            AVG(avg_tone),
            SUM(num_mentions),
            jsonb_build_object(
                'US', COUNT(*) FILTER (WHERE actor1_country='US' OR actor2_country='US'),
                'CN', COUNT(*) FILTER (WHERE actor1_country='CN' OR actor2_country='CN'),
                'RU', COUNT(*) FILTER (WHERE actor1_country='RU' OR actor2_country='RU'),
                'IR', COUNT(*) FILTER (WHERE actor1_country='IR' OR actor2_country='IR'),
                'TW', COUNT(*) FILTER (WHERE actor1_country='TW' OR actor2_country='TW'),
                'UA', COUNT(*) FILTER (WHERE actor1_country='UA' OR actor2_country='UA')
            )
        FROM gdelt_events
        GROUP BY event_date
        ON CONFLICT (summary_date) DO UPDATE SET
            total_events = EXCLUDED.total_events,
            conflict_count = EXCLUDED.conflict_count,
            cooperation_count = EXCLUDED.cooperation_count,
            avg_goldstein = EXCLUDED.avg_goldstein,
            avg_tone = EXCLUDED.avg_tone,
            total_mentions = EXCLUDED.total_mentions,
            country_events = EXCLUDED.country_events
    """)
    cur.execute("SELECT count(*) FROM gdelt_daily_summary")
    log.info("Daily summaries: {n} days", n=cur.fetchone()[0])


def push_to_resolved(cur):
    """Push GDELT daily features to resolved_series."""
    cur.execute(
        "INSERT INTO source_catalog (name,base_url,cost_tier,latency_class,pit_available,"
        "revision_behavior,trust_score,priority_rank) "
        "VALUES ('GDELT_BULK','https://data.gdeltproject.org','FREE','EOD',FALSE,"
        "'NEVER','MED',8) ON CONFLICT (name) DO NOTHING"
    )
    cur.execute("SELECT id FROM source_catalog WHERE name='GDELT_BULK'")
    src_id = cur.fetchone()[0]

    feat_ids = {}
    for feat_name, (family, desc) in FEATURES.items():
        cur.execute(
            "INSERT INTO feature_registry (name,family,description,transformation,"
            "transformation_version,lag_days,normalization,missing_data_policy,"
            "eligible_from_date,model_eligible) "
            "VALUES (%s,%s,%s,'RAW',1,0,'ZSCORE','FORWARD_FILL','2020-01-01',TRUE) "
            "ON CONFLICT (name) DO NOTHING RETURNING id",
            (feat_name, family, desc),
        )
        row = cur.fetchone()
        if row:
            feat_ids[feat_name] = row[0]
        else:
            cur.execute("SELECT id FROM feature_registry WHERE name=%s", (feat_name,))
            feat_ids[feat_name] = cur.fetchone()[0]

    # Push from daily summary
    cur.execute("SELECT summary_date, total_events, conflict_count, cooperation_count, "
                "avg_goldstein, avg_tone, total_mentions, country_events FROM gdelt_daily_summary")
    rows = cur.fetchall()

    total = 0
    for row in rows:
        d, total_ev, conflict, coop, goldstein, tone, mentions, country_ev = row
        d_str = d.isoformat() if hasattr(d, "isoformat") else str(d)

        insertions = [
            ("gdelt_total_events", total_ev),
            ("gdelt_conflict_count", conflict),
            ("gdelt_cooperation_count", coop),
            ("gdelt_avg_goldstein", goldstein),
            ("gdelt_avg_tone", tone),
            ("gdelt_num_mentions", mentions),
        ]
        if country_ev:
            for cc in TRACKED_COUNTRIES:
                val = country_ev.get(cc, 0)
                insertions.append((f"gdelt_events_{cc.lower()}", val))

        for feat_name, val in insertions:
            if val is None:
                continue
            fid = feat_ids.get(feat_name)
            if fid:
                cur.execute(
                    "INSERT INTO resolved_series (feature_id,obs_date,release_date,vintage_date,"
                    "value,source_priority_used) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                    (fid, d_str, d_str, d_str, float(val), src_id),
                )
                total += 1

    log.info("Pushed {n} GDELT feature points to resolved_series", n=total)


def main():
    if not BULK_DIR.exists():
        log.error("GDELT bulk directory not found: {d}", d=BULK_DIR)
        sys.exit(1)

    files = sorted(list(BULK_DIR.glob("*.zip")) + list(BULK_DIR.glob("*.csv")) + list(BULK_DIR.glob("*.CSV")))
    if not files:
        log.error("No GDELT files found in {d}", d=BULK_DIR)
        sys.exit(1)

    log.info("Found {n} GDELT files in {d}", n=len(files), d=BULK_DIR)

    conn = connect()
    conn.autocommit = True
    cur = conn.cursor()

    create_tables(cur)

    total_rows = 0
    for fp in files:
        log.info("Parsing {f}", f=fp.name)
        count = parse_csv_file(fp, cur)
        total_rows += count
        log.info("  {f}: {n} events", f=fp.name, n=count)

    log.info("Total events loaded: {n}", n=total_rows)

    build_daily_summaries(cur)
    push_to_resolved(cur)

    cur.execute("SELECT count(*) FROM gdelt_events")
    log.info("gdelt_events: {n} rows", n=cur.fetchone()[0])

    conn.close()
    log.info("GDELT parse complete")


if __name__ == "__main__":
    main()
