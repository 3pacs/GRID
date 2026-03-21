#!/usr/bin/env python3
"""GRID — Parse EIA energy data from pre-downloaded JSON files + API pulls.

Reads bulk EIA JSON from /data/grid/bulk/eia/ and also pulls additional
daily series (WTI, Brent, electricity) via the EIA API v2.

Creates 19 new EIA features in resolved_series.

Run: python3 parse_eia.py
"""

import os
import sys
import json
import time
from datetime import datetime
from pathlib import Path

import requests
import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings
from loguru import logger as log

BULK_DIR = Path("/data/grid/bulk/eia")
EIA_KEY = settings.EIA_API_KEY or os.getenv("EIA_API_KEY", "")

# Series to pull via API (not in bulk downloads)
API_SERIES = {
    "eia_wti_daily": {
        "route": "petroleum/pri/spt/data/",
        "params": {"frequency": "daily", "data[0]": "value", "facets[product][]": "EPCWTI", "sort[0][column]": "period", "sort[0][direction]": "desc", "length": 2000},
        "family": "commodity",
        "desc": "EIA WTI Crude Spot Price (daily)",
    },
    "eia_brent_daily": {
        "route": "petroleum/pri/spt/data/",
        "params": {"frequency": "daily", "data[0]": "value", "facets[product][]": "EPCBRENT", "sort[0][column]": "period", "sort[0][direction]": "desc", "length": 2000},
        "family": "commodity",
        "desc": "EIA Brent Crude Spot Price (daily)",
    },
    "eia_elec_gen_total": {
        "route": "electricity/rto/daily-fuel-type-data/data/",
        "params": {"frequency": "daily", "data[0]": "value", "facets[fueltype][]": "ALL", "facets[respondent][]": "US48", "sort[0][column]": "period", "sort[0][direction]": "desc", "length": 1000},
        "family": "macro",
        "desc": "EIA US Total Electricity Generation (daily MWh)",
    },
}

# Bulk JSON file patterns → feature mapping
BULK_FEATURES = {
    "PET.WCESTUS1.W": ("eia_crude_stocks", "commodity", "EIA US Crude Oil Stocks (weekly, barrels)"),
    "NG.NW2_EPG0_SWO_R48_BCF.W": ("eia_natgas_storage", "commodity", "EIA US Natural Gas Storage (weekly, Bcf)"),
    "PET.EMM_EPMR_PTE_NUS_DPG.W": ("eia_gasoline_price", "commodity", "EIA US Regular Gasoline Price (weekly, $/gal)"),
    "PET.EMD_EPD2DXL0_PTE_NUS_DPG.W": ("eia_diesel_price", "commodity", "EIA US Diesel Price (weekly, $/gal)"),
    "PET.MCRFPUS2.M": ("eia_crude_production", "commodity", "EIA US Crude Oil Production (monthly, kb/d)"),
    "PET.MTTIMUS1.M": ("eia_crude_imports", "commodity", "EIA US Crude Oil Imports (monthly, kb/d)"),
    "PET.MOPUEUS2.M": ("eia_refinery_util", "commodity", "EIA US Refinery Utilization Rate (monthly, %)"),
    "PET.RWTC.D": ("eia_wti_spot", "commodity", "EIA WTI Crude Spot Price (from bulk)"),
    "PET.RBRTE.D": ("eia_brent_spot", "commodity", "EIA Brent Crude Spot Price (from bulk)"),
    "NG.RNGWHHD.D": ("eia_natgas_henry_hub", "commodity", "EIA Henry Hub Natural Gas Spot (daily)"),
    "ELEC.GEN.ALL-US-99.M": ("eia_elec_gen_monthly", "macro", "EIA US Electricity Net Generation (monthly)"),
    "ELEC.GEN.SUN-US-99.M": ("eia_solar_gen", "macro", "EIA US Solar Generation (monthly)"),
    "ELEC.GEN.WND-US-99.M": ("eia_wind_gen", "macro", "EIA US Wind Generation (monthly)"),
    "ELEC.GEN.NG-US-99.M": ("eia_natgas_gen", "macro", "EIA US Natural Gas Generation (monthly)"),
    "ELEC.GEN.COL-US-99.M": ("eia_coal_gen", "macro", "EIA US Coal Generation (monthly)"),
    "ELEC.GEN.NUC-US-99.M": ("eia_nuclear_gen", "macro", "EIA US Nuclear Generation (monthly)"),
}


def connect():
    return psycopg2.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        dbname=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
    )


def get_or_create_source(cur):
    cur.execute(
        "INSERT INTO source_catalog (name,base_url,cost_tier,latency_class,pit_available,"
        "revision_behavior,trust_score,priority_rank) "
        "VALUES ('EIA','https://api.eia.gov/v2','FREE','EOD',TRUE,"
        "'RARE','HIGH',3) ON CONFLICT (name) DO NOTHING"
    )
    cur.execute("SELECT id FROM source_catalog WHERE name='EIA'")
    return cur.fetchone()[0]


def get_fid(cur, name, family, desc):
    cur.execute(
        "INSERT INTO feature_registry (name,family,description,transformation,"
        "transformation_version,lag_days,normalization,missing_data_policy,"
        "eligible_from_date,model_eligible) "
        "VALUES (%s,%s,%s,'RAW',1,0,'ZSCORE','FORWARD_FILL','2020-01-01',TRUE) "
        "ON CONFLICT (name) DO NOTHING RETURNING id",
        (name, family, desc),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("SELECT id FROM feature_registry WHERE name=%s", (name,))
    return cur.fetchone()[0]


def ins(cur, fid, obs_date, value, src_id):
    cur.execute(
        "INSERT INTO resolved_series (feature_id,obs_date,release_date,vintage_date,value,source_priority_used) "
        "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
        (fid, obs_date, obs_date, obs_date, value, src_id),
    )


def parse_bulk_files(cur, src_id):
    """Parse pre-downloaded EIA JSON files from bulk directory."""
    total = 0

    for json_file in sorted(BULK_DIR.glob("*.json")):
        log.info("Reading {f}", f=json_file.name)
        try:
            with open(json_file) as f:
                data = json.load(f)
        except Exception as e:
            log.warning("Failed to parse {f}: {e}", f=json_file.name, e=e)
            continue

        # EIA bulk JSON structure varies; handle both v1 and v2 formats
        series_list = []
        if isinstance(data, dict):
            if "series" in data:
                series_list = data["series"]
            elif "response" in data and "data" in data["response"]:
                series_list = [{"series_id": "api_response", "data": data["response"]["data"]}]
            elif "data" in data:
                series_list = [{"series_id": json_file.stem, "data": data["data"]}]

        for series in series_list:
            sid = series.get("series_id", "")
            if sid in BULK_FEATURES:
                feat_name, family, desc = BULK_FEATURES[sid]
                fid = get_fid(cur, feat_name, family, desc)
                points = series.get("data", [])
                count = 0
                for point in points:
                    if isinstance(point, (list, tuple)) and len(point) >= 2:
                        date_str, value = point[0], point[1]
                    elif isinstance(point, dict):
                        date_str = point.get("period", point.get("date", ""))
                        value = point.get("value", point.get("data", None))
                    else:
                        continue

                    if value is None or value == "":
                        continue
                    try:
                        value = float(value)
                    except (ValueError, TypeError):
                        continue

                    # Normalize date format
                    date_str = str(date_str).strip()
                    if len(date_str) == 6:  # YYYYMM
                        date_str = f"{date_str[:4]}-{date_str[4:6]}-01"
                    elif len(date_str) == 8:  # YYYYMMDD
                        date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                    elif "T" in date_str:
                        date_str = date_str[:10]

                    ins(cur, fid, date_str, value, src_id)
                    count += 1

                total += count
                if count:
                    log.info("  {sid} → {feat}: {n} points", sid=sid, feat=feat_name, n=count)

    return total


def pull_api_series(cur, src_id):
    """Pull additional EIA series via API v2."""
    if not EIA_KEY:
        log.warning("EIA_API_KEY not set, skipping API pulls")
        return 0

    total = 0
    base = "https://api.eia.gov/v2/"

    for feat_name, spec in API_SERIES.items():
        try:
            params = dict(spec["params"])
            params["api_key"] = EIA_KEY
            url = base + spec["route"]

            log.info("Pulling {feat} from API", feat=feat_name)
            r = requests.get(url, params=params, timeout=60)
            r.raise_for_status()
            data = r.json()

            points = data.get("response", {}).get("data", [])
            fid = get_fid(cur, feat_name, spec["family"], spec["desc"])

            count = 0
            for point in points:
                date_str = point.get("period", "")
                value = point.get("value")
                if not date_str or value is None:
                    continue
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    continue

                ins(cur, fid, date_str, value, src_id)
                count += 1

            total += count
            log.info("  {feat}: {n} points from API", feat=feat_name, n=count)
            time.sleep(1)

        except Exception as e:
            log.error("  {feat}: API error {e}", feat=feat_name, e=e)

    return total


def main():
    conn = connect()
    conn.autocommit = True
    cur = conn.cursor()

    src_id = get_or_create_source(cur)
    log.info("EIA source ID: {id}", id=src_id)

    bulk_count = 0
    if BULK_DIR.exists():
        bulk_count = parse_bulk_files(cur, src_id)
        log.info("Bulk files: {n} total points loaded", n=bulk_count)
    else:
        log.warning("Bulk dir not found: {d}", d=BULK_DIR)

    api_count = pull_api_series(cur, src_id)
    log.info("API pulls: {n} total points loaded", n=api_count)

    # Stats
    cur.execute("SELECT count(*) FROM resolved_series WHERE source_priority_used=%s", (src_id,))
    log.info("Total EIA points in resolved_series: {n}", n=cur.fetchone()[0])

    conn.close()
    log.info("EIA parse complete — {bulk} bulk + {api} API = {total} total",
             bulk=bulk_count, api=api_count, total=bulk_count + api_count)


if __name__ == "__main__":
    main()
