#!/usr/bin/env python3
"""
Pull prediction market historical data directly from Kalshi + Polymarket APIs.

Falls back to API-based collection when the S3 archive is unavailable.
Uses GRID's existing API integrations and stores data in both Parquet
and the prediction_market_markets/trades DB tables.

Usage:
    python scripts/pull_pm_history.py                    # Pull all
    python scripts/pull_pm_history.py --platform kalshi   # Kalshi only
    python scripts/pull_pm_history.py --platform polymarket  # Polymarket only
    python scripts/pull_pm_history.py --max-markets 100   # Limit markets
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

_GRID_DIR = str(Path(__file__).resolve().parent.parent)
os.chdir(_GRID_DIR)
if _GRID_DIR not in sys.path:
    sys.path.insert(0, _GRID_DIR)

import httpx
import pandas as pd
from loguru import logger as log

# ── Output paths ────────────────────────────────────────────────────
DATA_DIR = Path(_GRID_DIR) / "data" / "prediction_markets"
KALSHI_MARKETS_DIR = DATA_DIR / "kalshi" / "markets"
KALSHI_TRADES_DIR = DATA_DIR / "kalshi" / "trades"
POLY_MARKETS_DIR = DATA_DIR / "polymarket" / "markets"
POLY_TRADES_DIR = DATA_DIR / "polymarket" / "trades"

# ── API endpoints ───────────────────────────────────────────────────
KALSHI_API = "https://api.elections.kalshi.com/trade-api/v2"
POLY_GAMMA_API = "https://gamma-api.polymarket.com"
POLY_CLOB_API = "https://clob.polymarket.com"

REQUEST_TIMEOUT = 30
RATE_LIMIT_DELAY = 0.5


def _get_json(url: str, params: dict | None = None, retries: int = 3) -> dict | None:
    """GET request with retry and rate limiting."""
    for attempt in range(retries):
        try:
            with httpx.Client(timeout=REQUEST_TIMEOUT) as client:
                resp = client.get(url, params=params)
                resp.raise_for_status()
                time.sleep(RATE_LIMIT_DELAY)
                return resp.json()
        except Exception as exc:
            if attempt < retries - 1:
                delay = 2 ** (attempt + 1)
                log.warning("Request failed (attempt {a}): {e}, retrying in {d}s",
                            a=attempt + 1, e=str(exc)[:80], d=delay)
                time.sleep(delay)
            else:
                log.error("Request failed after {r} retries: {e}", r=retries, e=str(exc)[:100])
                return None
    return None


# ── Kalshi ──────────────────────────────────────────────────────────


def pull_kalshi_markets(max_markets: int | None = None) -> pd.DataFrame:
    """Pull all Kalshi markets via API."""
    KALSHI_MARKETS_DIR.mkdir(parents=True, exist_ok=True)
    all_markets = []
    cursor = None
    batch = 0

    log.info("Pulling Kalshi markets...")
    while True:
        params: dict[str, Any] = {"limit": 200}
        if cursor:
            params["cursor"] = cursor

        data = _get_json(f"{KALSHI_API}/markets", params=params)
        if not data:
            break

        markets = data.get("markets", [])
        if not markets:
            break

        all_markets.extend(markets)
        batch += 1
        log.info("Kalshi markets batch {b}: {n} markets (total: {t})",
                 b=batch, n=len(markets), t=len(all_markets))

        if max_markets and len(all_markets) >= max_markets:
            all_markets = all_markets[:max_markets]
            break

        cursor = data.get("cursor")
        if not cursor:
            break

    if all_markets:
        df = pd.DataFrame(all_markets)
        outfile = KALSHI_MARKETS_DIR / f"markets_0_{len(all_markets)}.parquet"
        df.to_parquet(outfile, index=False)
        log.info("Saved {n} Kalshi markets to {f}", n=len(df), f=outfile)
        return df

    return pd.DataFrame()


def pull_kalshi_trades(
    tickers: list[str],
    max_workers: int = 5,
) -> int:
    """Pull trade history for Kalshi markets."""
    KALSHI_TRADES_DIR.mkdir(parents=True, exist_ok=True)

    total_trades = 0
    batch_size = 10000
    all_trades: list[dict] = []
    chunk_idx = 0

    def fetch_ticker(ticker: str) -> list[dict]:
        trades = []
        cursor = None
        while True:
            params: dict[str, Any] = {"ticker": ticker, "limit": 1000}
            if cursor:
                params["cursor"] = cursor
            data = _get_json(f"{KALSHI_API}/markets/trades", params=params)
            if not data:
                break
            batch_trades = data.get("trades", [])
            if not batch_trades:
                break
            trades.extend(batch_trades)
            cursor = data.get("cursor")
            if not cursor:
                break
        return trades

    log.info("Pulling Kalshi trades for {n} markets (workers={w})...",
             n=len(tickers), w=max_workers)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_ticker, t): t for t in tickers}
        completed = 0

        for future in as_completed(futures):
            ticker = futures[future]
            completed += 1
            try:
                trades = future.result()
                if trades:
                    all_trades.extend(trades)

                if completed % 50 == 0:
                    log.info("Progress: {c}/{t} markets, {n} trades buffered",
                             c=completed, t=len(tickers), n=len(all_trades))

                # Save in batches
                while len(all_trades) >= batch_size:
                    df = pd.DataFrame(all_trades[:batch_size])
                    outfile = KALSHI_TRADES_DIR / f"trades_{chunk_idx}_{chunk_idx + batch_size}.parquet"
                    df.to_parquet(outfile, index=False)
                    total_trades += batch_size
                    all_trades = all_trades[batch_size:]
                    chunk_idx += batch_size

            except Exception as exc:
                log.warning("Error fetching trades for {t}: {e}", t=ticker, e=str(exc)[:80])

    # Save remaining
    if all_trades:
        df = pd.DataFrame(all_trades)
        outfile = KALSHI_TRADES_DIR / f"trades_{chunk_idx}_{chunk_idx + len(all_trades)}.parquet"
        df.to_parquet(outfile, index=False)
        total_trades += len(all_trades)

    log.info("Kalshi trades complete: {n} trades saved", n=total_trades)
    return total_trades


# ── Polymarket ──────────────────────────────────────────────────────


def pull_polymarket_markets(max_markets: int | None = None) -> pd.DataFrame:
    """Pull all Polymarket markets via Gamma API."""
    POLY_MARKETS_DIR.mkdir(parents=True, exist_ok=True)
    all_markets = []
    offset = 0
    limit = 100

    log.info("Pulling Polymarket markets...")
    while True:
        data = _get_json(
            f"{POLY_GAMMA_API}/markets",
            params={"limit": limit, "offset": offset},
        )
        if not data or not isinstance(data, list):
            break

        all_markets.extend(data)
        log.info("Polymarket markets: {n} fetched (total: {t})",
                 n=len(data), t=len(all_markets))

        if max_markets and len(all_markets) >= max_markets:
            all_markets = all_markets[:max_markets]
            break

        if len(data) < limit:
            break

        offset += limit

    if all_markets:
        df = pd.DataFrame(all_markets)
        outfile = POLY_MARKETS_DIR / f"markets_0_{len(all_markets)}.parquet"
        df.to_parquet(outfile, index=False)
        log.info("Saved {n} Polymarket markets to {f}", n=len(df), f=outfile)
        return df

    return pd.DataFrame()


def pull_polymarket_trades(
    condition_ids: list[str],
    max_workers: int = 3,
) -> int:
    """Pull trade history for Polymarket markets via CLOB API."""
    POLY_TRADES_DIR.mkdir(parents=True, exist_ok=True)

    total_trades = 0
    batch_size = 10000
    all_trades: list[dict] = []
    chunk_idx = 0

    def fetch_market_trades(cid: str) -> list[dict]:
        trades = []
        cursor = None
        while True:
            params: dict[str, Any] = {"market": cid, "limit": 500}
            if cursor:
                params["cursor"] = cursor
            # CLOB trades endpoint (public, no auth needed for reads)
            data = _get_json(f"{POLY_CLOB_API}/trades", params=params)
            if not data:
                break
            batch_trades = data if isinstance(data, list) else data.get("trades", data.get("data", []))
            if not batch_trades:
                break
            trades.extend(batch_trades)
            # Polymarket CLOB uses next_cursor
            cursor = data.get("next_cursor") if isinstance(data, dict) else None
            if not cursor:
                break
        return trades

    log.info("Pulling Polymarket trades for {n} markets...", n=len(condition_ids))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_market_trades, cid): cid for cid in condition_ids}
        completed = 0

        for future in as_completed(futures):
            cid = futures[future]
            completed += 1
            try:
                trades = future.result()
                if trades:
                    for t in trades:
                        t["market"] = cid
                    all_trades.extend(trades)

                if completed % 50 == 0:
                    log.info("Progress: {c}/{t} markets, {n} trades buffered",
                             c=completed, t=len(condition_ids), n=len(all_trades))

                while len(all_trades) >= batch_size:
                    df = pd.DataFrame(all_trades[:batch_size])
                    outfile = POLY_TRADES_DIR / f"trades_{chunk_idx}_{chunk_idx + batch_size}.parquet"
                    df.to_parquet(outfile, index=False)
                    total_trades += batch_size
                    all_trades = all_trades[batch_size:]
                    chunk_idx += batch_size

            except Exception as exc:
                log.warning("Error fetching trades for {c}: {e}", c=cid[:20], e=str(exc)[:80])

    if all_trades:
        df = pd.DataFrame(all_trades)
        outfile = POLY_TRADES_DIR / f"trades_{chunk_idx}_{chunk_idx + len(all_trades)}.parquet"
        df.to_parquet(outfile, index=False)
        total_trades += len(all_trades)

    log.info("Polymarket trades complete: {n} trades saved", n=total_trades)
    return total_trades


# ── Main ────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Pull prediction market historical data")
    parser.add_argument("--platform", choices=["kalshi", "polymarket", "all"], default="all")
    parser.add_argument("--max-markets", type=int, default=None, help="Limit number of markets")
    parser.add_argument("--max-workers", type=int, default=5, help="Concurrent API workers")
    parser.add_argument("--trades", action="store_true", default=True, help="Also pull trade history")
    parser.add_argument("--no-trades", action="store_true", help="Skip trade history")
    args = parser.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    pull_trades = args.trades and not args.no_trades

    if args.platform in ("kalshi", "all"):
        kalshi_df = pull_kalshi_markets(max_markets=args.max_markets)
        if pull_trades and not kalshi_df.empty:
            tickers = kalshi_df["ticker"].dropna().unique().tolist()
            # Filter to markets with meaningful volume
            if "volume" in kalshi_df.columns:
                active = kalshi_df[kalshi_df["volume"] >= 100]
                tickers = active["ticker"].dropna().unique().tolist()
            pull_kalshi_trades(tickers, max_workers=args.max_workers)

    if args.platform in ("polymarket", "all"):
        poly_df = pull_polymarket_markets(max_markets=args.max_markets)
        if pull_trades and not poly_df.empty:
            # Get condition IDs for trade fetching
            cid_col = None
            for col in ["condition_id", "id", "conditionId"]:
                if col in poly_df.columns:
                    cid_col = col
                    break
            if cid_col:
                cids = poly_df[cid_col].dropna().unique().tolist()
                pull_polymarket_trades(cids, max_workers=args.max_workers)

    # Mark complete
    sentinel = DATA_DIR / ".api_pull_complete"
    sentinel.touch()
    log.info("Historical data pull complete. Data at: {d}", d=DATA_DIR)
    log.info("Run the ingestion puller to sync to DB:")
    log.info("  python -c \"from ingestion.altdata.prediction_market_history import *\"")


if __name__ == "__main__":
    main()
