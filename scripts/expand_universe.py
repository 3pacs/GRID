#!/usr/bin/env python3
"""
Expand GRID's ticker universe — pull all liquid US equities.

Fetches S&P 500 constituents + additional high-volume names from Tiingo,
then backfills price data. Filters for >10K daily volume.

Usage:
  python3 scripts/expand_universe.py --list      # Show what's missing
  python3 scripts/expand_universe.py --pull       # Pull missing tickers
  python3 scripts/expand_universe.py --pull -n 50 # Pull 50 at a time (rate limits)
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date
from pathlib import Path

import pandas as pd
from loguru import logger as log

_GRID_DIR = str(Path(__file__).resolve().parent.parent)
os.chdir(_GRID_DIR)
if _GRID_DIR not in sys.path:
    sys.path.insert(0, _GRID_DIR)

from db import get_engine
from sqlalchemy import text

# S&P 500 components + high-volume names not already in GRID
# This is the "canonical" universe for the options scanner
SP500_CORE = [
    # Mega-cap tech
    "AAPL", "MSFT", "AMZN", "GOOGL", "GOOG", "META", "NVDA", "TSLA", "AVGO", "ADBE",
    "CRM", "ORCL", "AMD", "INTC", "QCOM", "TXN", "NOW", "INTU", "AMAT", "MU",
    "LRCX", "KLAC", "SNPS", "CDNS", "PANW", "CRWD", "FTNT", "DDOG", "ZS", "TEAM",
    # Financials
    "JPM", "BAC", "WFC", "GS", "MS", "C", "BLK", "SCHW", "AXP", "COF",
    "USB", "PNC", "TFC", "BK", "CME", "ICE", "MCO", "SPGI", "MSCI", "FIS",
    # Healthcare
    "UNH", "JNJ", "LLY", "PFE", "MRK", "ABBV", "TMO", "ABT", "DHR", "BMY",
    "AMGN", "GILD", "VRTX", "REGN", "ISRG", "MDT", "SYK", "BSX", "EW", "ZTS",
    # Consumer
    "WMT", "PG", "KO", "PEP", "COST", "HD", "MCD", "NKE", "SBUX", "TGT",
    "LOW", "TJX", "BKNG", "MAR", "HLT", "CMG", "ORLY", "AZO", "ROST", "DG",
    # Industrial
    "CAT", "DE", "GE", "HON", "BA", "RTX", "LMT", "GD", "NOC", "UPS",
    "FDX", "WM", "RSG", "EMR", "ETN", "ITW", "PH", "ROK", "IR", "AME",
    # Energy
    "XOM", "CVX", "COP", "EOG", "SLB", "MPC", "PSX", "VLO", "OXY", "DVN",
    "HAL", "BKR", "FANG", "HES", "KMI", "WMB", "OKE", "TRGP", "ET", "EPD",
    # Communication
    "DIS", "CMCSA", "NFLX", "T", "VZ", "TMUS", "CHTR", "EA", "TTWO", "WBD",
    # Real Estate
    "PLD", "AMT", "CCI", "SPG", "O", "PSA", "EQIX", "DLR", "WELL", "AVB",
    # Utilities
    "NEE", "DUK", "SO", "D", "AEP", "SRE", "EXC", "XEL", "ED", "WEC",
    # Materials
    "LIN", "APD", "SHW", "ECL", "FCX", "NEM", "DOW", "DD", "PPG", "VMC",
    # High-vol non-SP500
    "COIN", "MSTR", "PLTR", "RIVN", "LCID", "SOFI", "HOOD", "RBLX", "SNAP", "PINS",
    "U", "AI", "PATH", "BILL", "HUBS", "MDB", "NET", "SHOP", "SQ", "PYPL",
    "ROKU", "DASH", "ABNB", "UBER", "LYFT", "GRAB", "SE", "MELI", "NU", "CPNG",
    # Crypto-adjacent / miners
    "MARA", "RIOT", "CLSK", "HUT", "BITF", "CIFR",
    # Biotech/pharma movers
    "MRNA", "BNTX", "XBI",
    # China / ADRs
    "BABA", "JD", "PDD", "BIDU", "NIO", "XPEV", "LI", "TME", "BILI", "FUTU",
    # Semiconductors deep
    "ARM", "SMCI", "DELL", "HPE", "MRVL", "ON", "GFS", "NXPI", "SWKS", "MPWR",
    # AI / Cloud / SaaS
    "SNOW", "PLTR", "DDOG", "ESTC", "CFLT", "IOT", "DOCN", "DT", "S", "GTLB",
    # SPACs / high-vol small caps
    "SIRI", "RXT", "IONQ", "RGTI", "QUBT",
    # Defense / aero
    "HII", "TDG", "HWM", "AXON", "LHX",
    # REITs with big moves
    "IRM", "DLR", "EQIX",
    # Retail / consumer
    "LULU", "CROX", "DECK", "ON", "WDAY",
    # Insurance
    "KNSL", "ROOT", "LMND", "OSCR",
]

# Top crypto to pull via Tiingo crypto endpoint
CRYPTO_UNIVERSE = [
    "btcusd", "ethusd", "solusd", "bnbusd", "xrpusd", "adausd",
    "dogeusd", "avaxusd", "dotusd", "linkusd", "maticusd", "uniusd",
    "nearusd", "icpusd", "aptusd", "suiusd", "opusd", "arbusd",
    "mkrusd", "aaveusd", "ldusd", "snxusd", "compusd",
    "filusd", "atomusd", "injusd", "tiausd", "jupusd",
    "renderusd", "ftmusd", "pepeusd", "shibausd", "bonkusd",
    "taousd", "kasusd", "hbarusd", "algousd", "vetusd",
]

# Top FX pairs to pull via Tiingo FX endpoint
FX_UNIVERSE = [
    "eurusd", "gbpusd", "usdjpy", "audusd", "usdcad",
    "usdchf", "nzdusd", "eurgbp", "eurjpy", "gbpjpy",
    "usdmxn", "usdbrl", "usdcnh", "usdinr", "usdtry",
]

# ETFs and instruments already handled by yfinance_pull
ALREADY_HAVE_ETFS = {
    "SPY", "QQQ", "IWM", "DIA", "GLD", "SLV", "USO", "UNG", "TLT", "HYG",
    "LQD", "XLK", "XLF", "XLE", "XLV", "XLI", "XLY", "XLP", "XLU", "XLRE",
    "XLB", "XLC", "KRE", "XBI",
}


def get_existing_tickers(engine) -> set[str]:
    """Get tickers already in resolved_series."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT fr.name FROM feature_registry fr
            JOIN resolved_series rs ON fr.id = rs.feature_id
            WHERE fr.name LIKE '%_full'
        """)).fetchall()
    return {r[0].replace("_full", "").upper() for r in rows}


def main() -> None:
    parser = argparse.ArgumentParser(description="Expand GRID ticker universe")
    parser.add_argument("--list", action="store_true", help="Show missing tickers")
    parser.add_argument("--pull", action="store_true", help="Pull missing tickers via Tiingo")
    parser.add_argument("-n", type=int, default=50, help="Max tickers to pull per run (rate limits)")
    parser.add_argument("--start", default="2020-01-01", help="Backfill start date")
    args = parser.parse_args()

    engine = get_engine()
    existing = get_existing_tickers(engine)

    all_wanted = set(SP500_CORE) - ALREADY_HAVE_ETFS
    missing = all_wanted - existing
    have = all_wanted & existing

    log.info("Universe: {total} wanted, {have} already ingested, {miss} missing",
             total=len(all_wanted), have=len(have), miss=len(missing))

    if args.list or not args.pull:
        log.info("\nALREADY HAVE ({n}):", n=len(have))
        for t in sorted(have):
            log.info("  {t}", t=t)

        log.info("\nMISSING ({n}):", n=len(missing))
        for t in sorted(missing):
            log.info("  {t}", t=t)
        return

    if args.pull:
        missing_list = sorted(missing)[:args.n]
        log.info("Pulling {n} equity tickers via Tiingo (start={s})...",
                 n=len(missing_list), s=args.start)

        from ingestion.tiingo_pull import TiingoPuller

        puller = TiingoPuller(engine)
        results = puller.pull_all(
            ticker_list=missing_list,
            start_date=args.start,
        )

        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        failed = sum(1 for r in results if r["status"] == "FAILED")
        total_rows = sum(r.get("rows_inserted", 0) for r in results)

        log.info("\nRESULTS: {s} succeeded, {f} failed, {r} total rows inserted",
                 s=succeeded, f=failed, r=total_rows)

        if failed > 0:
            log.info("\nFailed tickers:")
            for r in results:
                if r["status"] == "FAILED":
                    log.info("  {t}: {e}", t=r["ticker"], e=r.get("errors", ["unknown"]))

        remaining = len(missing) - len(missing_list)
        if remaining > 0:
            log.info("\n{n} tickers still missing. Run again to continue.", n=remaining)


if __name__ == "__main__":
    main()
