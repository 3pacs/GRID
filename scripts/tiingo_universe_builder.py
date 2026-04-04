#!/usr/bin/env python3
"""
Tiingo Universe Builder — prioritize by daily dollar volume.

Tier 1: >$1M daily volume   → pull immediately
Tier 2: >$100K daily volume → pull second pass
Tier 3: >$10K daily volume  → pull third pass
Tier 4: anything with unusual volume spike → flag for monitoring

Uses Tiingo IEX endpoint for real-time volume/price (bulk query, fast).
Falls back to EOD endpoint for historical volume estimation.

Usage:
  python3 scripts/tiingo_universe_builder.py --scan       # Scan and tier all tickers
  python3 scripts/tiingo_universe_builder.py --pull-t1    # Pull Tier 1 (>$1M)
  python3 scripts/tiingo_universe_builder.py --pull-t2    # Pull Tier 2 (>$100K)
  python3 scripts/tiingo_universe_builder.py --pull-t3    # Pull Tier 3 (>$10K)
  python3 scripts/tiingo_universe_builder.py --spikes     # Detect unusual volume spikes
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from loguru import logger as log

_GRID_DIR = str(Path(__file__).resolve().parent.parent)
os.chdir(_GRID_DIR)
if _GRID_DIR not in sys.path:
    sys.path.insert(0, _GRID_DIR)

from db import get_engine
from sqlalchemy import text

_KEY = os.getenv("TIINGO_API_KEY", "")
_HEADERS = {"Authorization": f"Token {_KEY}", "Content-Type": "application/json"}
_DATA_DIR = Path("data")


def _iex_batch(tickers: list[str]) -> list[dict]:
    """Query Tiingo IEX for real-time quotes (up to 100 tickers per call)."""
    ticker_str = ",".join(tickers)
    url = f"https://api.tiingo.com/iex/?tickers={ticker_str}&token={_KEY}"
    try:
        r = requests.get(url, headers=_HEADERS, timeout=30)
        if r.ok:
            return r.json()
    except Exception:
        pass
    return []


def _eod_recent(ticker: str) -> dict | None:
    """Get last 5 trading days of EOD data for volume estimation."""
    start = (date.today() - timedelta(days=10)).isoformat()
    url = f"https://api.tiingo.com/tiingo/daily/{ticker}/prices"
    params = {"startDate": start, "token": _KEY}
    try:
        r = requests.get(url, params=params, headers=_HEADERS, timeout=15)
        if r.ok:
            data = r.json()
            if data:
                # Average the last few days
                volumes = [d.get("volume", 0) for d in data if d.get("volume")]
                prices = [d.get("close", 0) for d in data if d.get("close")]
                if volumes and prices:
                    avg_vol = sum(volumes) / len(volumes)
                    avg_px = sum(prices) / len(prices)
                    return {
                        "ticker": ticker,
                        "avg_volume": avg_vol,
                        "avg_price": avg_px,
                        "dollar_volume": avg_vol * avg_px,
                        "last_close": prices[-1],
                        "last_volume": volumes[-1],
                        "days_sampled": len(volumes),
                    }
    except Exception:
        pass
    return None


def scan_universe(exchange_filter: str | None = None) -> pd.DataFrame:
    """
    Scan all active tickers and estimate daily dollar volume.

    Uses IEX endpoint in batches of 100 for speed.
    """
    # Load active universe
    csv_path = _DATA_DIR / "tiingo_active_universe.csv"
    if not csv_path.exists():
        log.error("Run with no args first to download universe")
        return pd.DataFrame()

    df = pd.read_csv(csv_path)

    # Filter to stocks and ETFs only (skip mutual funds)
    df = df[df["assetType"].isin(["Stock", "ETF"])]

    # Filter to US exchanges
    us_exchanges = {"NYSE", "NASDAQ", "NYSE ARCA", "NYSE MKT", "BATS", "OTC"}
    if exchange_filter:
        df = df[df["exchange"].str.upper().str.contains(exchange_filter.upper(), na=False)]
    else:
        df = df[df["exchange"].fillna("").str.upper().isin(us_exchanges) |
                df["priceCurrency"].fillna("").str.upper().eq("USD")]

    log.info("Scanning {n} US stocks + ETFs via IEX...", n=len(df))

    tickers = [str(t) for t in df["ticker"].tolist() if pd.notna(t) and str(t).strip()]
    all_quotes = []

    # IEX supports batch queries — 100 tickers at a time
    batch_size = 100
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        quotes = _iex_batch(batch)

        for q in quotes:
            ticker = q.get("ticker", "")
            last_price = q.get("tngoLast") or q.get("last") or q.get("prevClose", 0)
            last_vol = q.get("volume", 0) or 0

            if last_price and last_price > 0:
                dollar_vol = last_vol * last_price
                all_quotes.append({
                    "ticker": ticker,
                    "last_price": last_price,
                    "volume": last_vol,
                    "dollar_volume": dollar_vol,
                    "prev_close": q.get("prevClose", 0),
                    "mid": q.get("mid", 0),
                    "timestamp": q.get("timestamp", ""),
                })

        if (i // batch_size) % 10 == 0:
            log.info("  Scanned {i}/{n} tickers, {q} with quotes...",
                     i=i + batch_size, n=len(tickers), q=len(all_quotes))

        time.sleep(0.1)  # Light rate limiting

    quotes_df = pd.DataFrame(all_quotes)
    if quotes_df.empty:
        log.warning("No quotes returned")
        return quotes_df

    quotes_df = quotes_df.sort_values("dollar_volume", ascending=False)

    # Tier assignment
    quotes_df["tier"] = "T4"
    quotes_df.loc[quotes_df["dollar_volume"] >= 10_000, "tier"] = "T3"
    quotes_df.loc[quotes_df["dollar_volume"] >= 100_000, "tier"] = "T2"
    quotes_df.loc[quotes_df["dollar_volume"] >= 1_000_000, "tier"] = "T1"

    # Summary
    tier_counts = quotes_df["tier"].value_counts().sort_index()
    log.info("\nTIER SUMMARY:")
    for tier, count in tier_counts.items():
        label = {"T1": ">$1M/day", "T2": ">$100K/day", "T3": ">$10K/day", "T4": "<$10K/day"}
        log.info("  {t}: {n:5d} tickers  ({l})", t=tier, n=count, l=label.get(tier, ""))

    # Save
    out_path = _DATA_DIR / "tiingo_universe_tiered.csv"
    quotes_df.to_csv(out_path, index=False)
    log.info("\nSaved tiered universe to {p}", p=out_path)

    # Show top of each tier
    for tier in ["T1", "T2", "T3"]:
        subset = quotes_df[quotes_df["tier"] == tier]
        log.info("\n{t} TOP 10:", t=tier)
        for _, row in subset.head(10).iterrows():
            log.info("  {t:8s} ${px:>10.2f}  vol={v:>12,.0f}  $vol={dv:>14,.0f}",
                     t=row["ticker"], px=row["last_price"],
                     v=row["volume"], dv=row["dollar_volume"])

    return quotes_df


def pull_tier(tier: str, batch_size: int = 50) -> None:
    """Pull EOD price data for all tickers in a tier."""
    csv_path = _DATA_DIR / "tiingo_universe_tiered.csv"
    if not csv_path.exists():
        log.error("Run --scan first")
        return

    df = pd.read_csv(csv_path)
    tier_df = df[df["tier"] == tier]

    engine = get_engine()

    # Check which we already have
    with engine.connect() as conn:
        existing = conn.execute(text("""
            SELECT DISTINCT UPPER(REPLACE(fr.name, '_full', ''))
            FROM feature_registry fr
            JOIN resolved_series rs ON fr.id = rs.feature_id
            WHERE fr.name LIKE '%_full'
        """)).fetchall()
    existing_set = {r[0] for r in existing}

    tickers = [t for t in tier_df["ticker"].tolist() if t.upper() not in existing_set]
    log.info("{t}: {total} tickers, {have} already ingested, {need} to pull",
             t=tier, total=len(tier_df), have=len(tier_df) - len(tickers), need=len(tickers))

    if not tickers:
        log.info("Nothing to pull for {t}", t=tier)
        return

    from ingestion.tiingo_pull import TiingoPuller
    puller = TiingoPuller(engine)

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        log.info("Pulling batch {b}/{total} ({n} tickers)...",
                 b=i // batch_size + 1, total=(len(tickers) + batch_size - 1) // batch_size,
                 n=len(batch))

        results = puller.pull_all(ticker_list=batch, start_date="2020-01-01")
        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        rows = sum(r.get("rows_inserted", 0) for r in results)
        log.info("  Batch done: {s}/{n} succeeded, {r} rows", s=succeeded, n=len(batch), r=rows)


def detect_volume_spikes(lookback_days: int = 5, spike_threshold: float = 3.0) -> pd.DataFrame:
    """
    Detect tickers with unusual volume spikes (volume > threshold × average).

    Uses IEX real-time data compared to recent average.
    """
    csv_path = _DATA_DIR / "tiingo_universe_tiered.csv"
    if not csv_path.exists():
        log.error("Run --scan first")
        return pd.DataFrame()

    df = pd.read_csv(csv_path)
    # Only check tickers with meaningful volume
    df = df[df["dollar_volume"] >= 10_000]

    log.info("Checking {n} tickers for volume spikes...", n=len(df))

    spikes = []
    tickers = df["ticker"].tolist()

    for i in range(0, len(tickers), 100):
        batch = tickers[i:i + 100]
        quotes = _iex_batch(batch)

        for q in quotes:
            ticker = q.get("ticker", "")
            current_vol = q.get("volume", 0) or 0
            prev_close = q.get("prevClose", 0) or 0

            # Compare to our stored average
            row = df[df["ticker"] == ticker]
            if row.empty:
                continue

            avg_vol = row.iloc[0].get("volume", 0)
            if avg_vol and avg_vol > 0 and current_vol > 0:
                vol_ratio = current_vol / avg_vol
                if vol_ratio >= spike_threshold:
                    price = q.get("tngoLast") or q.get("last") or prev_close
                    spikes.append({
                        "ticker": ticker,
                        "current_volume": current_vol,
                        "avg_volume": avg_vol,
                        "volume_ratio": vol_ratio,
                        "price": price,
                        "dollar_volume": current_vol * price if price else 0,
                    })

        time.sleep(0.1)

    spike_df = pd.DataFrame(spikes).sort_values("volume_ratio", ascending=False)

    if not spike_df.empty:
        log.info("\nVOLUME SPIKES DETECTED ({n}):", n=len(spike_df))
        for _, row in spike_df.head(30).iterrows():
            log.info(
                "  {t:8s} ${px:>8.2f}  vol={v:>12,.0f}  avg={a:>12,.0f}  ratio={r:>5.1f}x  $vol={dv:>12,.0f}",
                t=row["ticker"], px=row["price"],
                v=row["current_volume"], a=row["avg_volume"],
                r=row["volume_ratio"], dv=row["dollar_volume"],
            )

        spike_path = _DATA_DIR / f"volume_spikes_{date.today()}.csv"
        spike_df.to_csv(spike_path, index=False)
        log.info("Saved to {p}", p=spike_path)

    return spike_df


def main() -> None:
    parser = argparse.ArgumentParser(description="Tiingo Universe Builder")
    parser.add_argument("--scan", action="store_true", help="Scan and tier all tickers by volume")
    parser.add_argument("--pull-t1", action="store_true", help="Pull Tier 1 (>$1M daily vol)")
    parser.add_argument("--pull-t2", action="store_true", help="Pull Tier 2 (>$100K daily vol)")
    parser.add_argument("--pull-t3", action="store_true", help="Pull Tier 3 (>$10K daily vol)")
    parser.add_argument("--spikes", action="store_true", help="Detect volume spikes")
    parser.add_argument("--all", action="store_true", help="Scan + pull all tiers")
    parser.add_argument("--batch", type=int, default=50, help="Batch size for pulls")
    args = parser.parse_args()

    if not _KEY:
        log.error("Set TIINGO_API_KEY")
        return

    if args.scan or args.all:
        scan_universe()

    if args.pull_t1 or args.all:
        pull_tier("T1", args.batch)

    if args.pull_t2 or args.all:
        pull_tier("T2", args.batch)

    if args.pull_t3 or args.all:
        pull_tier("T3", args.batch)

    if args.spikes:
        detect_volume_spikes()


if __name__ == "__main__":
    main()
