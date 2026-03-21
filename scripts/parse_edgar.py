#!/usr/bin/env python3
"""GRID — Parse SEC EDGAR XBRL quarterly zip files into PostgreSQL.

Reads pre-downloaded XBRL financial statements (2009-2025) from /data/grid/bulk/edgar/,
extracts key fundamentals (revenue, net income, assets, debt, cash, equity),
and pushes aggregate time-series into resolved_series.

Tables created:
  - edgar_submissions: Company metadata (CIK, name, SIC, filing dates)
  - edgar_numeric: Numeric XBRL facts (tag, value, date, units)

Run: python3 parse_edgar.py
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

BULK_DIR = Path("/data/grid/bulk/edgar")

# XBRL tags we care about for aggregate features
TARGET_TAGS = {
    "Revenues": "edgar_agg_revenue",
    "RevenueFromContractWithCustomerExcludingAssessedTax": "edgar_agg_revenue",
    "SalesRevenueNet": "edgar_agg_revenue",
    "NetIncomeLoss": "edgar_agg_net_income",
    "Assets": "edgar_agg_assets",
    "LongTermDebt": "edgar_agg_debt",
    "LongTermDebtNoncurrent": "edgar_agg_debt",
    "CashAndCashEquivalentsAtCarryingValue": "edgar_agg_cash",
    "StockholdersEquity": "edgar_agg_equity",
}

FEATURE_DEFS = {
    "edgar_agg_revenue": ("macro", "EDGAR Aggregate Revenue (quarterly filings)"),
    "edgar_agg_net_income": ("macro", "EDGAR Aggregate Net Income (quarterly filings)"),
    "edgar_agg_assets": ("macro", "EDGAR Aggregate Total Assets"),
    "edgar_agg_debt": ("macro", "EDGAR Aggregate Long-Term Debt"),
    "edgar_agg_cash": ("macro", "EDGAR Aggregate Cash & Equivalents"),
    "edgar_agg_equity": ("macro", "EDGAR Aggregate Stockholders Equity"),
}


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
        CREATE TABLE IF NOT EXISTS edgar_submissions (
            id          BIGSERIAL PRIMARY KEY,
            cik         INTEGER NOT NULL,
            company     TEXT,
            sic         INTEGER,
            form        TEXT,
            filed       DATE,
            period      DATE,
            adsh        TEXT UNIQUE,
            created_at  TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_edgar_sub_cik ON edgar_submissions (cik);
        CREATE INDEX IF NOT EXISTS idx_edgar_sub_period ON edgar_submissions (period);
        CREATE INDEX IF NOT EXISTS idx_edgar_sub_form ON edgar_submissions (form);

        CREATE TABLE IF NOT EXISTS edgar_numeric (
            id      BIGSERIAL PRIMARY KEY,
            adsh    TEXT NOT NULL,
            tag     TEXT NOT NULL,
            version TEXT,
            ddate   DATE,
            qtrs    INTEGER,
            value   DOUBLE PRECISION,
            uom     TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_edgar_num_adsh ON edgar_numeric (adsh);
        CREATE INDEX IF NOT EXISTS idx_edgar_num_tag ON edgar_numeric (tag);
        CREATE INDEX IF NOT EXISTS idx_edgar_num_ddate ON edgar_numeric (ddate);
    """)


def parse_zip(zip_path, cur):
    """Parse a single EDGAR quarterly zip (sub.txt + num.txt)."""
    subs_loaded = 0
    nums_loaded = 0

    with zipfile.ZipFile(zip_path, "r") as zf:
        names = zf.namelist()

        # Parse submissions (sub.txt)
        if "sub.txt" in names:
            with zf.open("sub.txt") as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"), delimiter="\t")
                for row in reader:
                    try:
                        cur.execute(
                            "INSERT INTO edgar_submissions (adsh,cik,company,sic,form,filed,period) "
                            "VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (adsh) DO NOTHING",
                            (
                                row.get("adsh", "").strip(),
                                int(row.get("cik", 0)),
                                row.get("name", "")[:500],
                                int(row["sic"]) if row.get("sic", "").strip() else None,
                                row.get("form", ""),
                                row.get("filed", None),
                                row.get("period", None),
                            ),
                        )
                        subs_loaded += 1
                    except Exception:
                        continue

        # Parse numeric facts (num.txt)
        if "num.txt" in names:
            with zf.open("num.txt") as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"), delimiter="\t")
                batch = []
                for row in reader:
                    tag = row.get("tag", "")
                    if tag not in TARGET_TAGS:
                        continue
                    val = row.get("value", "")
                    if not val:
                        continue
                    try:
                        batch.append((
                            row.get("adsh", "").strip(),
                            tag,
                            row.get("version", ""),
                            row.get("ddate", None),
                            int(row["qtrs"]) if row.get("qtrs", "").strip() else None,
                            float(val),
                            row.get("uom", ""),
                        ))
                    except (ValueError, KeyError):
                        continue

                    if len(batch) >= 5000:
                        _insert_batch(cur, batch)
                        nums_loaded += len(batch)
                        batch = []

                if batch:
                    _insert_batch(cur, batch)
                    nums_loaded += len(batch)

    return subs_loaded, nums_loaded


def _insert_batch(cur, batch):
    args = ",".join(
        cur.mogrify("(%s,%s,%s,%s,%s,%s,%s)", row).decode() for row in batch
    )
    cur.execute(
        "INSERT INTO edgar_numeric (adsh,tag,version,ddate,qtrs,value,uom) VALUES " + args
        + " ON CONFLICT DO NOTHING"
    )


def aggregate_to_resolved(cur):
    """Build quarterly aggregate features from edgar_numeric and push to resolved_series."""
    # Ensure source exists
    cur.execute(
        "INSERT INTO source_catalog (name,base_url,cost_tier,latency_class,pit_available,"
        "revision_behavior,trust_score,priority_rank) "
        "VALUES ('SEC_EDGAR_BULK','https://www.sec.gov/dera/data','FREE','MONTHLY',TRUE,"
        "'RARE','HIGH',4) ON CONFLICT (name) DO NOTHING"
    )
    cur.execute("SELECT id FROM source_catalog WHERE name='SEC_EDGAR_BULK'")
    src_id = cur.fetchone()[0]

    # Register features
    feat_ids = {}
    for feat_name, (family, desc) in FEATURE_DEFS.items():
        cur.execute(
            "INSERT INTO feature_registry (name,family,description,transformation,"
            "transformation_version,lag_days,normalization,missing_data_policy,"
            "eligible_from_date,model_eligible) "
            "VALUES (%s,%s,%s,'AGG_SUM',1,0,'ZSCORE','FORWARD_FILL','2009-01-01',TRUE) "
            "ON CONFLICT (name) DO NOTHING RETURNING id",
            (feat_name, family, desc),
        )
        row = cur.fetchone()
        if row:
            feat_ids[feat_name] = row[0]
        else:
            cur.execute("SELECT id FROM feature_registry WHERE name=%s", (feat_name,))
            feat_ids[feat_name] = cur.fetchone()[0]

    # Aggregate: sum values per quarter per tag, push to resolved_series
    for xbrl_tag, feat_name in TARGET_TAGS.items():
        fid = feat_ids.get(feat_name)
        if not fid:
            continue
        cur.execute(
            """
            INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value, source_priority_used)
            SELECT %s, date_trunc('quarter', n.ddate)::date, MAX(s.filed), MAX(s.filed),
                   SUM(n.value), %s
            FROM edgar_numeric n
            JOIN edgar_submissions s ON s.adsh = n.adsh
            WHERE n.tag = %s AND n.qtrs = 1 AND n.uom = 'USD'
              AND n.ddate IS NOT NULL AND s.filed IS NOT NULL
            GROUP BY date_trunc('quarter', n.ddate)
            ON CONFLICT (feature_id, obs_date, vintage_date) DO UPDATE SET value = EXCLUDED.value
            """,
            (fid, src_id, xbrl_tag),
        )
        log.info("Aggregated {tag} → {feat}", tag=xbrl_tag, feat=feat_name)


def main():
    if not BULK_DIR.exists():
        log.error("EDGAR bulk directory not found: {d}", d=BULK_DIR)
        sys.exit(1)

    zips = sorted(BULK_DIR.glob("*.zip"))
    if not zips:
        log.error("No zip files found in {d}", d=BULK_DIR)
        sys.exit(1)

    log.info("Found {n} EDGAR zip files in {d}", n=len(zips), d=BULK_DIR)

    conn = connect()
    conn.autocommit = True
    cur = conn.cursor()

    create_tables(cur)

    total_subs = 0
    total_nums = 0
    for zp in zips:
        log.info("Parsing {f}", f=zp.name)
        subs, nums = parse_zip(zp, cur)
        total_subs += subs
        total_nums += nums
        log.info("  {f}: {s} submissions, {n} numeric facts", f=zp.name, s=subs, n=nums)

    log.info("Total: {s} submissions, {n} numeric facts", s=total_subs, n=total_nums)

    log.info("Aggregating to resolved_series...")
    aggregate_to_resolved(cur)

    # Final stats
    cur.execute("SELECT count(*) FROM edgar_submissions")
    log.info("edgar_submissions: {n} rows", n=cur.fetchone()[0])
    cur.execute("SELECT count(*) FROM edgar_numeric")
    log.info("edgar_numeric: {n} rows", n=cur.fetchone()[0])

    conn.close()
    log.info("EDGAR parse complete")


if __name__ == "__main__":
    main()
