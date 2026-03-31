#!/usr/bin/env python3
"""
Bulk historical price loader — download ZIP/CSV dumps instead of API-crawling.

Sources:
  1. Kaggle: 9000+ tickers full history (stocks/ETFs)
     https://www.kaggle.com/datasets/jakewright/9000-tickers-of-stock-market-data-full-history
  2. Kaggle: Huge Stock Market Dataset (all US stocks + ETFs)
     https://www.kaggle.com/datasets/borismarjanovic/price-volume-data-for-all-us-stocks-etfs
  3. Kraken: Full OHLCVT history for all crypto pairs (official ZIP)
     https://support.kraken.com/articles/360047124832
  4. CryptoDataDownload: Pre-built CSVs for major exchanges
     https://www.cryptodatadownload.com/data/

Usage:
    # Download Kaggle datasets (requires kaggle CLI configured)
    python3 scripts/bulk_download_prices.py --source kaggle

    # Download Kraken crypto data
    python3 scripts/bulk_download_prices.py --source kraken

    # Load already-downloaded CSVs into the database
    python3 scripts/bulk_download_prices.py --load /path/to/csv_dir
"""

from __future__ import annotations

import argparse
import glob
import os
import subprocess
import sys
from datetime import date
from pathlib import Path

import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

_GRID_DIR = str(Path(__file__).resolve().parent.parent)
os.chdir(_GRID_DIR)
if _GRID_DIR not in sys.path:
    sys.path.insert(0, _GRID_DIR)

from db import get_engine

DOWNLOAD_DIR = Path("/data/grid/bulk_prices")

# Kaggle dataset slugs
KAGGLE_DATASETS = [
    "jakewright/9000-tickers-of-stock-market-data-full-history",
    "borismarjanovic/price-volume-data-for-all-us-stocks-etfs",
]

# Kraken official OHLCVT bulk download
KRAKEN_OHLCVT_URL = "https://support.kraken.com/articles/360047124832-downloadable-historical-ohlcvt-open-high-low-close-volume-trades-data"

# CryptoDataDownload base
CRYPTO_CSV_BASE = "https://www.cryptodatadownload.com/cdd"


def download_kaggle(dataset_slug: str, dest: Path) -> Path:
    """Download a Kaggle dataset ZIP."""
    dest.mkdir(parents=True, exist_ok=True)
    name = dataset_slug.split("/")[-1]
    out_dir = dest / name

    if out_dir.exists() and any(out_dir.iterdir()):
        log.info("Kaggle dataset already downloaded: {d}", d=out_dir)
        return out_dir

    log.info("Downloading Kaggle dataset: {s}", s=dataset_slug)
    result = subprocess.run(
        ["kaggle", "datasets", "download", "-d", dataset_slug, "-p", str(dest), "--unzip"],
        capture_output=True, text=True, timeout=600,
    )
    if result.returncode != 0:
        log.error("Kaggle download failed: {e}", e=result.stderr)
        raise RuntimeError(f"Kaggle download failed: {result.stderr}")

    log.info("Downloaded to {d}", d=dest)
    return dest


def download_crypto_csv(pair: str, exchange: str = "Kraken") -> pd.DataFrame:
    """Download a single crypto pair CSV from CryptoDataDownload."""
    import requests

    url = f"{CRYPTO_CSV_BASE}/{exchange}-{pair}-d.csv"
    log.info("Fetching {u}", u=url)

    resp = requests.get(url, timeout=30)
    if resp.status_code != 200:
        log.warning("Failed to fetch {p}: {s}", p=pair, s=resp.status_code)
        return pd.DataFrame()

    # CryptoDataDownload CSVs have a header comment line
    lines = resp.text.strip().split("\n")
    if lines[0].startswith("http"):
        lines = lines[1:]

    from io import StringIO
    df = pd.read_csv(StringIO("\n".join(lines)))
    return df


def load_csvs_to_db(csv_dir: str, engine: Engine, source_name: str = "BULK_IMPORT") -> int:
    """Load OHLCV CSVs into raw_series.

    Expects CSVs with columns like: Date, Open, High, Low, Close, Volume
    (or similar — we auto-detect common column patterns).
    """
    csv_files = glob.glob(os.path.join(csv_dir, "**/*.csv"), recursive=True)
    if not csv_files:
        log.warning("No CSV files found in {d}", d=csv_dir)
        return 0

    log.info("Found {n} CSV files in {d}", n=len(csv_files), d=csv_dir)

    # Get or create source_id
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT id FROM source_catalog WHERE name = :n"), {"n": source_name}
        ).fetchone()
        if row:
            source_id = row[0]
        else:
            r = conn.execute(text(
                "INSERT INTO source_catalog (name, base_url, cost_tier, latency_class, "
                "pit_available, revision_behavior, trust_score, priority_rank) "
                "VALUES (:n, :u, 'FREE', 'EOD', TRUE, 'NEVER', 'HIGH', 5) RETURNING id"
            ), {"n": source_name, "u": "bulk_import"})
            source_id = r.fetchone()[0]

    total_inserted = 0
    failed = 0

    for csv_path in csv_files:
        try:
            ticker = Path(csv_path).stem.upper()

            # Skip index files, metadata
            if ticker.startswith(".") or ticker.startswith("_"):
                continue

            df = pd.read_csv(csv_path, parse_dates=True)

            # Auto-detect date column
            date_col = None
            for candidate in ["Date", "date", "timestamp", "Timestamp", "datetime"]:
                if candidate in df.columns:
                    date_col = candidate
                    break
            if date_col is None:
                continue

            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
            df = df.dropna(subset=[date_col])

            # Auto-detect OHLCV columns (case-insensitive)
            col_map = {}
            for col in df.columns:
                lower = col.lower()
                if lower in ("open",):
                    col_map[col] = "open"
                elif lower in ("high",):
                    col_map[col] = "high"
                elif lower in ("low",):
                    col_map[col] = "low"
                elif lower in ("close", "adj close", "adj_close", "adjclose"):
                    col_map[col] = "close"
                elif lower in ("volume", "vol"):
                    col_map[col] = "volume"

            if "close" not in col_map.values():
                continue  # Need at least close price

            rows = []
            for _, row in df.iterrows():
                obs_date = row[date_col].date() if hasattr(row[date_col], "date") else row[date_col]
                for src_col, field in col_map.items():
                    val = row.get(src_col)
                    if pd.notna(val):
                        rows.append({
                            "sid": f"YF:{ticker}:{field}",
                            "src": source_id,
                            "od": obs_date,
                            "val": float(val),
                        })

            if rows:
                with engine.begin() as conn:
                    conn.execute(
                        text(
                            "INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_status) "
                            "VALUES (:sid, :src, :od, :val, 'SUCCESS') "
                            "ON CONFLICT (series_id, source_id, obs_date, pull_timestamp) DO NOTHING"
                        ),
                        rows,
                    )
                total_inserted += len(rows)

            if total_inserted % 100_000 == 0 and total_inserted > 0:
                log.info("Progress: {n} rows inserted...", n=total_inserted)

        except Exception as e:
            failed += 1
            if failed <= 5:
                log.warning("Failed to load {f}: {e}", f=csv_path, e=str(e))

    log.info(
        "Bulk load complete: {n} rows inserted from {f} files ({e} failed)",
        n=total_inserted, f=len(csv_files), e=failed,
    )
    return total_inserted


def main() -> None:
    parser = argparse.ArgumentParser(description="Bulk historical price loader")
    parser.add_argument("--source", choices=["kaggle", "kraken", "crypto"], help="Download source")
    parser.add_argument("--load", type=str, help="Load CSVs from this directory")
    args = parser.parse_args()

    engine = get_engine()

    if args.source == "kaggle":
        DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
        for slug in KAGGLE_DATASETS:
            try:
                download_kaggle(slug, DOWNLOAD_DIR)
            except Exception as e:
                log.error("Failed {s}: {e}", s=slug, e=str(e))
        load_csvs_to_db(str(DOWNLOAD_DIR), engine, source_name="KAGGLE_BULK")

    elif args.source == "kraken" or args.source == "crypto":
        pairs = ["BTCUSD", "ETHUSD", "SOLUSD", "ADAUSD", "AVAXUSD", "DOTUSD", "LINKUSD"]
        dest = DOWNLOAD_DIR / "crypto"
        dest.mkdir(parents=True, exist_ok=True)
        for pair in pairs:
            df = download_crypto_csv(pair)
            if not df.empty:
                df.to_csv(dest / f"{pair}.csv", index=False)
        load_csvs_to_db(str(dest), engine, source_name="CRYPTO_BULK")

    elif args.load:
        load_csvs_to_db(args.load, engine)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
