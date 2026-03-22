#!/usr/bin/env python3
"""GRID — Pull intraday price bars and save as parquet.

Pulls 1-min (7d), 5-min (60d), 1-hour (730d) bars for 16 tickers via yfinance.
Saves parquet files to /data/grid/intraday/.
Computes intraday features: volatility, VWAP deviation, range, volume skew, close location.

Run: python3 pull_intraday.py
"""

import os
import sys
from datetime import datetime, date, timedelta
from pathlib import Path

import psycopg2
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings
from loguru import logger as log

try:
    import yfinance as yf
except ImportError:
    log.error("yfinance not installed: pip install yfinance")
    sys.exit(1)

OUTPUT_DIR = Path("/data/grid/intraday")

TICKERS = {
    "^GSPC": "sp500",
    "^IXIC": "nasdaq",
    "^RUT": "russell2000",
    "SPY": "spy",
    "QQQ": "qqq",
    "IWM": "iwm",
    "BTC-USD": "btc",
    "ETH-USD": "eth",
    "SOL-USD": "sol",
    "GC=F": "gold",
    "CL=F": "crude_oil",
    "DX-Y.NYB": "dxy",
    "TLT": "tlt",
    "HYG": "hyg",
    "XLE": "xle",
    "VIX": "vix",
}

INTERVALS = [
    ("1m", "7d", "1min"),
    ("5m", "60d", "5min"),
    ("1h", "730d", "1hour"),
]


def connect():
    return psycopg2.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        dbname=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
    )


def compute_intraday_features(df, name):
    """Compute daily intraday features from OHLCV bars."""
    if df.empty or "Close" not in df.columns:
        return {}

    # Flatten multi-index columns if present
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]

    df = df.copy()
    df["date"] = df.index.date

    features = {}
    for dt, group in df.groupby("date"):
        if len(group) < 2:
            continue

        high = group["High"].max()
        low = group["Low"].min()
        close = group["Close"].iloc[-1]
        open_price = group["Open"].iloc[0]
        volume = group["Volume"].sum() if "Volume" in group.columns else 0

        # Intraday range (high-low as pct of open)
        intraday_range = (high - low) / open_price if open_price > 0 else 0

        # Intraday volatility (std of returns)
        returns = group["Close"].pct_change().dropna()
        intraday_vol = returns.std() if len(returns) > 1 else 0

        # VWAP deviation
        if "Volume" in group.columns and group["Volume"].sum() > 0:
            vwap = (group["Close"] * group["Volume"]).sum() / group["Volume"].sum()
            vwap_dev = (close - vwap) / vwap if vwap > 0 else 0
        else:
            vwap_dev = 0

        # Volume skew (first half vs second half)
        mid = len(group) // 2
        if mid > 0 and "Volume" in group.columns:
            vol_first = group["Volume"].iloc[:mid].sum()
            vol_second = group["Volume"].iloc[mid:].sum()
            vol_skew = (vol_first - vol_second) / (vol_first + vol_second) if (vol_first + vol_second) > 0 else 0
        else:
            vol_skew = 0

        # Close location value (where close sits in day's range)
        clv = (close - low) / (high - low) if (high - low) > 0 else 0.5

        features[str(dt)] = {
            "intraday_range": intraday_range,
            "intraday_vol": intraday_vol,
            "vwap_dev": vwap_dev,
            "volume_skew": vol_skew,
            "close_location": clv,
        }

    return features


def push_features(cur, name, features, src_id):
    """Push computed intraday features to resolved_series."""
    feature_defs = {
        f"{name}_intraday_range": ("vol", f"{name} Intraday Range (H-L/O)"),
        f"{name}_intraday_vol": ("vol", f"{name} Intraday Volatility (return std)"),
        f"{name}_vwap_dev": ("vol", f"{name} VWAP Deviation at Close"),
        f"{name}_volume_skew": ("sentiment", f"{name} Volume Skew (AM vs PM)"),
        f"{name}_close_location": ("vol", f"{name} Close Location Value"),
    }

    feat_ids = {}
    for feat_name, (family, desc) in feature_defs.items():
        cur.execute(
            "INSERT INTO feature_registry (name,family,description,transformation,"
            "transformation_version,lag_days,normalization,missing_data_policy,"
            "eligible_from_date,model_eligible) "
            "VALUES (%s,%s,%s,'INTRADAY',1,0,'ZSCORE','FORWARD_FILL','2024-04-01',TRUE) "
            "ON CONFLICT (name) DO NOTHING RETURNING id",
            (feat_name, family, desc),
        )
        row = cur.fetchone()
        if row:
            feat_ids[feat_name] = row[0]
        else:
            cur.execute("SELECT id FROM feature_registry WHERE name=%s", (feat_name,))
            feat_ids[feat_name] = cur.fetchone()[0]

    count = 0
    for dt_str, vals in features.items():
        for suffix, val in vals.items():
            feat_name = f"{name}_{suffix}"
            fid = feat_ids.get(feat_name)
            if fid and val is not None:
                cur.execute(
                    "INSERT INTO resolved_series (feature_id,obs_date,release_date,vintage_date,"
                    "value,source_priority_used) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                    (fid, dt_str, dt_str, dt_str, float(val), src_id),
                )
                count += 1

    return count


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    conn = connect()
    conn.autocommit = True
    cur = conn.cursor()

    # Ensure source
    cur.execute(
        "INSERT INTO source_catalog (name,base_url,cost_tier,latency_class,pit_available,"
        "revision_behavior,trust_score,priority_rank) "
        "VALUES ('YFINANCE_INTRADAY','https://finance.yahoo.com','FREE','REALTIME',FALSE,"
        "'NEVER','MED',7) ON CONFLICT (name) DO NOTHING"
    )
    cur.execute("SELECT id FROM source_catalog WHERE name='YFINANCE_INTRADAY'")
    src_id = cur.fetchone()[0]

    total_features = 0

    for yf_ticker, name in TICKERS.items():
        log.info("Pulling {name} ({ticker})", name=name, ticker=yf_ticker)

        for interval, period, label in INTERVALS:
            try:
                df = yf.download(yf_ticker, period=period, interval=interval, progress=False)
                if df.empty:
                    log.warning("  {name} {label}: no data", name=name, label=label)
                    continue

                # Save parquet
                pq_path = OUTPUT_DIR / f"{name}_{label}.parquet"
                df.to_parquet(pq_path)
                log.info("  {name} {label}: {n} bars → {p}",
                         name=name, label=label, n=len(df), p=pq_path.name)

                # Compute and push features from hourly data (best balance of history + granularity)
                if interval == "1h":
                    features = compute_intraday_features(df, name)
                    if features:
                        count = push_features(cur, name, features, src_id)
                        total_features += count
                        log.info("  {name}: {n} feature points pushed", name=name, n=count)

            except Exception as e:
                log.error("  {name} {label}: {e}", name=name, label=label, e=e)

    log.info("Intraday pull complete: {n} feature points pushed", n=total_features)

    # Disk usage
    total_size = sum(f.stat().st_size for f in OUTPUT_DIR.glob("*.parquet"))
    log.info("Parquet files: {s:.1f} MB in {d}", s=total_size / 1e6, d=OUTPUT_DIR)

    conn.close()


if __name__ == "__main__":
    main()
