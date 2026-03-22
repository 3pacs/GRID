"""
GRID — Unified data migration and bulk loading script.

Migrates DuckDB hypotheses/flywheel scores to PostgreSQL,
loads GDELT zips and EIA JSON from /data/grid/bulk/ into resolved_series,
and bridges the DuckDB live ingest sources (NY Fed, Kalshi, Treasury, OFR)
into PostgreSQL raw_series.

Usage:
    python scripts/migrate_and_load.py --all
    python scripts/migrate_and_load.py --duckdb
    python scripts/migrate_and_load.py --gdelt
    python scripts/migrate_and_load.py --eia
    python scripts/migrate_and_load.py --live-sources
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import zipfile
from datetime import date, datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path

import pandas as pd
from loguru import logger as log
from sqlalchemy import text

# Add parent dir so we can import grid modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db import get_engine

# Paths
BULK_DIR = Path("/data/grid/bulk")
DUCKDB_PATH = Path("/data/grid/duckdb/grid.duckdb")
GDELT_DIR = BULK_DIR / "gdelt"
EIA_DIR = BULK_DIR / "eia" / "series"


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────

def ensure_source(engine, name: str, url: str = "", **kwargs) -> int:
    """Get or create a source_catalog entry, return its ID."""
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT id FROM source_catalog WHERE name = :n"), {"n": name}
        ).fetchone()
        if row:
            return row[0]
    # Create it
    defaults = {
        "base_url": url or f"https://{name.lower()}.example.com",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": False,
        "revision_behavior": "NEVER",
        "trust_score": "MED",
        "priority_rank": 50,
        "active": True,
    }
    defaults.update(kwargs)
    with engine.begin() as conn:
        result = conn.execute(
            text("""
                INSERT INTO source_catalog
                (name, base_url, cost_tier, latency_class, pit_available,
                 revision_behavior, trust_score, priority_rank, active)
                VALUES (:name, :base_url, :cost_tier, :latency_class, :pit_available,
                        :revision_behavior, :trust_score, :priority_rank, :active)
                ON CONFLICT (name) DO NOTHING
                RETURNING id
            """),
            {"name": name, **defaults},
        )
        row = result.fetchone()
        if row:
            return row[0]
    # If ON CONFLICT hit, fetch again
    with engine.connect() as conn:
        return conn.execute(
            text("SELECT id FROM source_catalog WHERE name = :n"), {"n": name}
        ).fetchone()[0]


def ensure_feature(engine, name: str, family: str, description: str,
                   transformation: str = "RAW", normalization: str = "RAW") -> int:
    """Get or create a feature_registry entry, return its ID."""
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT id FROM feature_registry WHERE name = :n"), {"n": name}
        ).fetchone()
        if row:
            return row[0]
    with engine.begin() as conn:
        result = conn.execute(
            text("""
                INSERT INTO feature_registry
                (name, family, description, transformation, normalization,
                 missing_data_policy, eligible_from_date, model_eligible)
                VALUES (:name, :family, :desc, :transformation, :normalization,
                        'FORWARD_FILL', '2020-01-01', TRUE)
                ON CONFLICT (name) DO NOTHING
                RETURNING id
            """),
            {
                "name": name, "family": family, "desc": description,
                "transformation": transformation, "normalization": normalization,
            },
        )
        row = result.fetchone()
        if row:
            return row[0]
    with engine.connect() as conn:
        return conn.execute(
            text("SELECT id FROM feature_registry WHERE name = :n"), {"n": name}
        ).fetchone()[0]


def insert_resolved(engine, feature_id: int, source_id: int,
                    obs_date: date, value: float) -> bool:
    """Insert into resolved_series if not duplicate. Returns True if inserted."""
    try:
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO resolved_series
                    (feature_id, obs_date, release_date, vintage_date, value,
                     source_priority_used, conflict_flag)
                    VALUES (:fid, :od, :od, :od, :val, :src, FALSE)
                    ON CONFLICT (feature_id, obs_date, vintage_date) DO NOTHING
                """),
                {"fid": feature_id, "od": obs_date, "val": value, "src": source_id},
            )
        return True
    except Exception:
        return False


# ──────────────────────────────────────────────────────────────────────
# 1. DuckDB → PostgreSQL migration
# ──────────────────────────────────────────────────────────────────────

def migrate_duckdb(engine):
    """Migrate hypotheses and flywheel scores from DuckDB to PostgreSQL."""
    log.info("=== Migrating DuckDB → PostgreSQL ===")

    try:
        import duckdb
    except ImportError:
        log.error("duckdb not installed — pip install duckdb")
        return

    if not DUCKDB_PATH.exists():
        log.warning("DuckDB not found at {p}", p=DUCKDB_PATH)
        return

    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)

    # Migrate hypotheses
    hyps = con.execute("SELECT * FROM hypothesis_registry ORDER BY id").fetchdf()
    log.info("Found {n} hypotheses in DuckDB", n=len(hyps))

    migrated = 0
    for _, h in hyps.iterrows():
        statement = h["statement"]
        category = h.get("category", "TACTICAL")

        # Map DuckDB categories to GRID layers
        layer_map = {
            "BTC_PROJECTION": "TACTICAL",
            "EQUITY_VALUE": "TACTICAL",
            "BUYOUT_ARBITRAGE": "TACTICAL",
            "DISTRESSED_TURNAROUND": "TACTICAL",
            "FLYWHEEL": "REGIME",
            "HISTORICAL_ANALOG": "TACTICAL",
            "HALVING_SCARCITY": "TACTICAL",
            "CORRELATION_REGIME": "REGIME",
            "VALUE_GAP": "TACTICAL",
        }
        layer = layer_map.get(category, "TACTICAL")

        # Map DuckDB status to GRID state
        status_map = {
            "SUPPORTED": "PASSED",
            "PARTIALLY_SUPPORTED": "TESTING",
            "UNTESTED": "CANDIDATE",
            "CONTRADICTED": "FAILED",
        }
        state = status_map.get(h.get("status", "UNTESTED"), "CANDIDATE")

        # Check if already exists
        with engine.connect() as conn:
            exists = conn.execute(
                text("SELECT 1 FROM hypothesis_registry WHERE statement = :s"),
                {"s": statement},
            ).fetchone()
            if exists:
                continue

        try:
            with engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO hypothesis_registry
                        (statement, layer, feature_ids, lag_structure,
                         proposed_metric, proposed_threshold, state)
                        VALUES (:statement, :layer, :fids, :lags,
                                :metric, :threshold, :state)
                    """),
                    {
                        "statement": statement,
                        "layer": layer,
                        "fids": "{}",  # empty array — legacy data
                        "lags": json.dumps({"notes": h.get("data_json", "{}"),
                                           "test": h.get("test", ""),
                                           "implication": h.get("implication", ""),
                                           "category": category}),
                        "metric": "sharpe",
                        "threshold": 0.5,
                        "state": state,
                    },
                )
            migrated += 1
        except Exception as exc:
            log.warning("Failed to migrate hypothesis '{s}': {e}",
                       s=statement[:60], e=str(exc))

    log.info("Migrated {n} hypotheses from DuckDB", n=migrated)

    # Migrate flywheel scores as decision journal entries
    fw = con.execute("SELECT * FROM flywheel_scores").fetchdf()
    log.info("Found {n} flywheel scores in DuckDB", n=len(fw))

    fw_migrated = 0
    for _, f in fw.iterrows():
        asset = f["asset"]
        score = f["score"]
        thesis = f.get("thesis", "")
        category = f.get("category", "")

        with engine.connect() as conn:
            exists = conn.execute(
                text("SELECT 1 FROM decision_journal WHERE action_taken = :a"),
                {"a": f"FLYWHEEL_{asset}"},
            ).fetchone()
            if exists:
                continue

        try:
            with engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO decision_journal
                        (model_version_id, inferred_state, state_confidence,
                         transition_probability, grid_recommendation,
                         baseline_recommendation, action_taken, counterfactual)
                        VALUES (1, :state, :conf, 0.0, :rec, :baseline, :action, :counter)
                    """),
                    {
                        "state": category,
                        "conf": score / 100.0,
                        "rec": thesis,
                        "baseline": "HOLD",
                        "action": f"FLYWHEEL_{asset}",
                        "counter": f"Score: {score}/100. {f.get('mechanical_value', '')}",
                    },
                )
            fw_migrated += 1
        except Exception as exc:
            log.warning("Failed to migrate flywheel {a}: {e}", a=asset, e=str(exc))

    log.info("Migrated {n} flywheel scores as journal entries", n=fw_migrated)

    # Migrate raw_ingest data to raw_series
    raw = con.execute("SELECT * FROM raw_ingest").fetchdf()
    log.info("Found {n} raw ingest records in DuckDB", n=len(raw))

    source_map = {
        "ny_fed_sofr": ("NY_FED", "rates", "ny_fed_sofr"),
        "kalshi": ("KALSHI", "sentiment", "kalshi_markets"),
        "fred_DFF": ("FRED", "rates", "fed_funds_rate"),
    }

    raw_migrated = 0
    for _, r in raw.iterrows():
        source_name = r["source"]
        if source_name not in source_map:
            continue
        cat_name, family, feature_name = source_map[source_name]
        try:
            payload = json.loads(r["payload"]) if isinstance(r["payload"], str) else r["payload"]
            src_id = ensure_source(engine, cat_name, f"https://{cat_name.lower()}.gov")

            # Extract values from payload
            if source_name == "ny_fed_sofr":
                rates = payload.get("refRates", [])
                for rate in rates:
                    obs = date.fromisoformat(rate["effectiveDate"])
                    val = float(rate.get("percentRate", 0))
                    fid = ensure_feature(engine, feature_name, family,
                                        "NY Fed SOFR rate")
                    if insert_resolved(engine, fid, src_id, obs, val):
                        raw_migrated += 1

            elif source_name == "fred_DFF":
                obs_list = payload.get("observations", [])
                for obs in obs_list:
                    try:
                        obs_date_val = date.fromisoformat(obs["date"])
                        val = float(obs["value"])
                        fid = ensure_feature(engine, feature_name, family,
                                            "Fed funds effective rate")
                        if insert_resolved(engine, fid, src_id, obs_date_val, val):
                            raw_migrated += 1
                    except (ValueError, KeyError):
                        continue

        except Exception as exc:
            log.debug("Failed to process raw ingest {s}: {e}", s=source_name, e=str(exc))

    log.info("Migrated {n} raw ingest records to resolved_series", n=raw_migrated)
    con.close()


# ──────────────────────────────────────────────────────────────────────
# 2. GDELT bulk loader — /data/grid/bulk/gdelt/*.zip → resolved_series
# ──────────────────────────────────────────────────────────────────────

def load_gdelt(engine):
    """Load GDELT daily zip files into resolved_series."""
    log.info("=== Loading GDELT bulk data ===")

    if not GDELT_DIR.exists():
        log.warning("GDELT dir not found at {p}", p=GDELT_DIR)
        return

    zips = sorted(GDELT_DIR.glob("*.zip"))
    log.info("Found {n} GDELT zip files", n=len(zips))

    src_id = ensure_source(engine, "GDELT", "https://api.gdeltproject.org",
                           trust_score="MED", latency_class="EOD")

    # Define GDELT features
    features = {
        "gdelt_avg_tone": ensure_feature(engine, "gdelt_avg_tone", "sentiment",
                                         "GDELT average news tone (positive=good)"),
        "gdelt_article_count": ensure_feature(engine, "gdelt_article_count", "sentiment",
                                              "GDELT total article count per day"),
        "gdelt_conflict_count": ensure_feature(engine, "gdelt_conflict_count", "sentiment",
                                               "GDELT conflict-related article count"),
    }

    # CAMEO conflict codes
    conflict_themes = {"PROTEST", "FIGHT", "ASSAULT", "COERCE", "FORCE", "MILITARY",
                       "CONFLICT", "THREATEN", "VIOLENCE", "KILL"}

    total_inserted = 0
    skipped = 0

    for i, zp in enumerate(zips):
        # Extract date from filename: 20240101.zip
        date_str = zp.stem
        try:
            obs = date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
        except (ValueError, IndexError):
            continue

        # Check if already loaded
        with engine.connect() as conn:
            exists = conn.execute(
                text("""SELECT 1 FROM resolved_series
                        WHERE feature_id = :fid AND obs_date = :od LIMIT 1"""),
                {"fid": features["gdelt_avg_tone"], "od": obs},
            ).fetchone()
            if exists:
                skipped += 1
                continue

        try:
            with zipfile.ZipFile(zp) as zf:
                for name in zf.namelist():
                    with zf.open(name) as f:
                        content = f.read().decode("utf-8", errors="ignore")

            # Parse tab-separated GKG data
            lines = content.strip().split("\n")
            tones = []
            conflict_count = 0

            for line in lines:
                fields = line.split("\t")
                # GKG field 7 = TONE (AvgTone,PosTone,NegTone,...)
                if len(fields) > 7:
                    try:
                        tone_parts = fields[7].split(",")
                        if tone_parts:
                            tones.append(float(tone_parts[0]))
                    except (ValueError, IndexError):
                        pass
                # GKG field 3 = THEMES — check for conflict themes
                if len(fields) > 3:
                    themes_str = fields[3].upper()
                    if any(ct in themes_str for ct in conflict_themes):
                        conflict_count += 1

            # Insert
            day_inserted = 0
            if tones:
                avg_tone = sum(tones) / len(tones)
                if insert_resolved(engine, features["gdelt_avg_tone"], src_id, obs, avg_tone):
                    day_inserted += 1

            if insert_resolved(engine, features["gdelt_article_count"], src_id, obs, float(len(lines))):
                day_inserted += 1

            if insert_resolved(engine, features["gdelt_conflict_count"], src_id, obs, float(conflict_count)):
                day_inserted += 1

            total_inserted += day_inserted

        except Exception as exc:
            log.debug("Failed to process GDELT {d}: {e}", d=date_str, e=str(exc))

        if (i + 1) % 100 == 0:
            log.info("GDELT progress: {i}/{n} files, {t} rows inserted, {s} skipped",
                    i=i + 1, n=len(zips), t=total_inserted, s=skipped)

    log.info("GDELT complete: {t} rows inserted, {s} skipped from {n} files",
            t=total_inserted, s=skipped, n=len(zips))


# ──────────────────────────────────────────────────────────────────────
# 3. EIA JSON loader — /data/grid/bulk/eia/series/*.json → resolved_series
# ──────────────────────────────────────────────────────────────────────

def load_eia(engine):
    """Load EIA energy data JSON files into resolved_series."""
    log.info("=== Loading EIA energy data ===")

    if not EIA_DIR.exists():
        log.warning("EIA series dir not found at {p}", p=EIA_DIR)
        return

    jsons = sorted(EIA_DIR.glob("*.json"))
    log.info("Found {n} EIA JSON files", n=len(jsons))

    src_id = ensure_source(engine, "EIA", "https://api.eia.gov",
                           trust_score="HIGH", latency_class="WEEKLY")

    # Map EIA series to features
    eia_features = {
        "crude_price_full": ("commodity", "eia_crude_price", "EIA crude oil spot price"),
        "ng_RNGWHHD": ("commodity", "eia_natgas_henry_hub", "EIA Henry Hub natural gas spot price"),
        "ng_RNGC1": ("commodity", "eia_natgas_futures_1m", "EIA natural gas futures front month"),
        "ng_RNGC4": ("commodity", "eia_natgas_futures_4m", "EIA natural gas futures 4-month"),
        "pet_WCRIMUS2": ("commodity", "eia_crude_imports", "EIA weekly crude oil imports"),
        "pet_WDIRPUS2": ("commodity", "eia_distillate_production", "EIA distillate fuel production"),
        "pet_WCRFPUS2": ("commodity", "eia_crude_refinery_input", "EIA crude refinery inputs"),
        "pet_WTTSTUS1": ("commodity", "eia_crude_stocks", "EIA crude oil total stocks"),
        "pet_WDISTUS1": ("commodity", "eia_distillate_stocks", "EIA distillate fuel stocks"),
        "pet_WKJSTUS1": ("commodity", "eia_jet_fuel_stocks", "EIA kerosene jet fuel stocks"),
        "pet_WPULEUS3": ("commodity", "eia_gasoline_production", "EIA motor gasoline production"),
        "elec_ELEC_GEN_ALL-US-99_M": ("macro", "eia_electricity_total", "EIA total US electricity generation"),
        "elec_ELEC_GEN_NG-US-99_M": ("macro", "eia_electricity_natgas", "EIA natural gas electricity generation"),
        "elec_ELEC_GEN_COL-US-99_M": ("macro", "eia_electricity_coal", "EIA coal electricity generation"),
        "elec_ELEC_GEN_NUC-US-99_M": ("macro", "eia_electricity_nuclear", "EIA nuclear electricity generation"),
        "elec_ELEC_GEN_WND-US-99_M": ("macro", "eia_electricity_wind", "EIA wind electricity generation"),
        "elec_ELEC_GEN_SUN-US-99_M": ("macro", "eia_electricity_solar", "EIA solar electricity generation"),
    }

    total_inserted = 0
    for jp in jsons:
        series_key = jp.stem
        if series_key not in eia_features:
            log.debug("Skipping unknown EIA series: {k}", k=series_key)
            continue

        family, feature_name, description = eia_features[series_key]
        fid = ensure_feature(engine, feature_name, family, description)

        try:
            with open(jp) as f:
                data = json.load(f)

            # EIA JSON format varies: could be {series: [{data: [[date, value], ...]}]}
            # or {response: {data: [...]}} or raw list
            records = []

            if isinstance(data, dict):
                # Try EIA v2 format
                if "response" in data and "data" in data["response"]:
                    for row in data["response"]["data"]:
                        try:
                            period = row.get("period", "")
                            value = row.get("value")
                            if value is not None and period:
                                # Handle monthly (YYYY-MM) or daily (YYYY-MM-DD)
                                if len(period) == 7:
                                    obs = date.fromisoformat(period + "-01")
                                else:
                                    obs = date.fromisoformat(period)
                                records.append((obs, float(value)))
                        except (ValueError, TypeError):
                            continue
                # Try EIA v1 format
                elif "series" in data:
                    for series in data["series"]:
                        for point in series.get("data", []):
                            try:
                                date_str, val = point[0], point[1]
                                if val is None:
                                    continue
                                # Handle YYYYMMDD or YYYY-MM-DD or YYYYMM
                                if len(str(date_str)) == 8:
                                    obs = date(int(str(date_str)[:4]),
                                              int(str(date_str)[4:6]),
                                              int(str(date_str)[6:8]))
                                elif len(str(date_str)) == 6:
                                    obs = date(int(str(date_str)[:4]),
                                              int(str(date_str)[4:6]), 1)
                                else:
                                    obs = date.fromisoformat(str(date_str)[:10])
                                records.append((obs, float(val)))
                            except (ValueError, TypeError, IndexError):
                                continue
                # Try flat data list
                elif "data" in data:
                    for row in data["data"]:
                        try:
                            if isinstance(row, list) and len(row) >= 2:
                                obs = date.fromisoformat(str(row[0])[:10])
                                records.append((obs, float(row[1])))
                            elif isinstance(row, dict):
                                period = row.get("period", row.get("date", ""))
                                value = row.get("value")
                                if value is not None and period:
                                    obs = date.fromisoformat(str(period)[:10])
                                    records.append((obs, float(value)))
                        except (ValueError, TypeError):
                            continue

            inserted = 0
            for obs, val in records:
                if insert_resolved(engine, fid, src_id, obs, val):
                    inserted += 1

            if inserted > 0:
                log.info("EIA {k}: {n} rows inserted", k=series_key, n=inserted)
            total_inserted += inserted

        except Exception as exc:
            log.warning("Failed to load EIA {k}: {e}", k=series_key, e=str(exc))

    log.info("EIA complete: {t} total rows inserted from {n} files",
            t=total_inserted, n=len(jsons))


# ──────────────────────────────────────────────────────────────────────
# 4. Live sources bridge — pull NY Fed/Kalshi/Treasury/OFR into PG
# ──────────────────────────────────────────────────────────────────────

def load_live_sources(engine):
    """Pull live data from NY Fed, Kalshi, Treasury, OFR into PostgreSQL."""
    log.info("=== Loading live sources ===")

    import requests

    def fetch_json(url, params=None, timeout=30):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.warning("Failed to fetch {u}: {e}", u=url, e=str(e))
            return None

    total = 0

    # --- NY Fed SOFR ---
    src_nyfed = ensure_source(engine, "NY_FED", "https://markets.newyorkfed.org",
                              trust_score="HIGH", latency_class="EOD")
    fid_sofr = ensure_feature(engine, "ny_fed_sofr", "rates",
                              "NY Fed SOFR overnight rate")
    data = fetch_json("https://markets.newyorkfed.org/api/rates/secured/sofr/last/30.json")
    if data and "refRates" in data:
        for rate in data["refRates"]:
            try:
                obs = date.fromisoformat(rate["effectiveDate"])
                val = float(rate["percentRate"])
                if insert_resolved(engine, fid_sofr, src_nyfed, obs, val):
                    total += 1
            except (ValueError, KeyError):
                continue
        log.info("NY Fed SOFR: loaded {n} rates", n=len(data["refRates"]))

    # --- NY Fed SOMA holdings ---
    fid_soma = ensure_feature(engine, "ny_fed_soma_holdings", "rates",
                              "NY Fed SOMA total holdings (billions)")
    data = fetch_json("https://markets.newyorkfed.org/api/soma/summary.json")
    if data and "soma" in data:
        soma = data["soma"]
        if "summary" in soma:
            for item in (soma["summary"] if isinstance(soma["summary"], list) else [soma["summary"]]):
                try:
                    obs = date.fromisoformat(item.get("asOfDate", "")[:10])
                    val = float(item.get("total", 0))
                    if insert_resolved(engine, fid_soma, src_nyfed, obs, val):
                        total += 1
                except (ValueError, KeyError):
                    pass
        log.info("NY Fed SOMA loaded")

    # --- NY Fed Repo operations ---
    fid_repo = ensure_feature(engine, "ny_fed_repo_volume", "rates",
                              "NY Fed repo operation total volume (billions)")
    week_ago = (date.today() - timedelta(days=30)).isoformat()
    today = date.today().isoformat()
    data = fetch_json(f"https://markets.newyorkfed.org/api/rp/results/search.json?startDate={week_ago}&endDate={today}")
    if data and "repo" in data:
        ops = data["repo"].get("operations", [])
        for op in ops:
            try:
                obs = date.fromisoformat(op.get("operationDate", "")[:10])
                val = float(op.get("totalAmtAccepted", 0))
                if insert_resolved(engine, fid_repo, src_nyfed, obs, val):
                    total += 1
            except (ValueError, KeyError):
                continue
        log.info("NY Fed Repo: {n} operations", n=len(ops))

    # --- Treasury TGA ---
    src_treasury = ensure_source(engine, "TREASURY", "https://api.fiscaldata.treasury.gov",
                                 trust_score="HIGH", latency_class="EOD")
    fid_tga = ensure_feature(engine, "treasury_tga_balance", "rates",
                             "Treasury General Account closing balance (billions)")
    data = fetch_json(
        "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/dts/deposits_withdrawals_operating_cash",
        {"sort": "-record_date", "page[size]": "60",
         "filter": "account_type:eq:Federal Reserve Account"}
    )
    if data and "data" in data:
        for row in data["data"]:
            try:
                obs = date.fromisoformat(row["record_date"])
                val = float(row.get("close_today_bal", 0))
                if insert_resolved(engine, fid_tga, src_treasury, obs, val):
                    total += 1
            except (ValueError, KeyError):
                continue
        log.info("Treasury TGA: {n} records", n=len(data["data"]))

    # --- OFR Financial Stress Index ---
    fred_key = os.environ.get("FRED_API_KEY", "")
    if fred_key:
        src_ofr = ensure_source(engine, "OFR", "https://www.financialresearch.gov",
                                trust_score="HIGH", latency_class="WEEKLY")
        fid_ofr = ensure_feature(engine, "ofr_financial_stress", "credit",
                                 "OFR Financial Stress Index (STLFSI2)")
        data = fetch_json(
            "https://api.stlouisfed.org/fred/series/observations",
            {"series_id": "STLFSI2", "api_key": fred_key, "file_type": "json",
             "sort_order": "desc", "limit": "100"}
        )
        if data and "observations" in data:
            for obs_item in data["observations"]:
                try:
                    obs = date.fromisoformat(obs_item["date"])
                    val = float(obs_item["value"])
                    if insert_resolved(engine, fid_ofr, src_ofr, obs, val):
                        total += 1
                except (ValueError, KeyError):
                    continue
            log.info("OFR FSI: {n} observations", n=len(data["observations"]))
    else:
        log.warning("No FRED_API_KEY set — skipping OFR FSI")

    log.info("Live sources complete: {t} total rows inserted", t=total)


# ──────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="GRID data migration and bulk loading")
    parser.add_argument("--all", action="store_true", help="Run all loaders")
    parser.add_argument("--duckdb", action="store_true", help="Migrate DuckDB → PostgreSQL")
    parser.add_argument("--gdelt", action="store_true", help="Load GDELT zip files")
    parser.add_argument("--eia", action="store_true", help="Load EIA JSON series")
    parser.add_argument("--live-sources", action="store_true", help="Pull NY Fed/Kalshi/Treasury/OFR")
    args = parser.parse_args()

    if not any([args.all, args.duckdb, args.gdelt, args.eia, args.live_sources]):
        args.all = True

    engine = get_engine()
    log.info("Connected to PostgreSQL")

    if args.all or args.duckdb:
        migrate_duckdb(engine)

    if args.all or args.gdelt:
        load_gdelt(engine)

    if args.all or args.eia:
        load_eia(engine)

    if args.all or args.live_sources:
        load_live_sources(engine)

    # Final counts
    with engine.connect() as conn:
        resolved = conn.execute(text("SELECT COUNT(*) FROM resolved_series")).scalar()
        features = conn.execute(text("SELECT COUNT(DISTINCT feature_id) FROM resolved_series")).scalar()
        hyps = conn.execute(text("SELECT COUNT(*) FROM hypothesis_registry")).scalar()
        journal = conn.execute(text("SELECT COUNT(*) FROM decision_journal")).scalar()

    log.info("=== FINAL STATE ===")
    log.info("  Resolved series: {n:,}", n=resolved)
    log.info("  Features with data: {n}", n=features)
    log.info("  Hypotheses: {n}", n=hyps)
    log.info("  Journal entries: {n}", n=journal)


if __name__ == "__main__":
    main()
