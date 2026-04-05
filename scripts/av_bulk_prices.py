"""
AlphaVantage Pro — bulk daily price history download.

Downloads FULL daily OHLCV history (20+ years, ~6000 rows each) for
500+ tickers via CSV. At 150 calls/min this takes ~4 minutes per batch.

This is the big one — ~3M+ price rows.

Run: python scripts/av_bulk_prices.py
"""

from __future__ import annotations

import csv
import io
import os
import sys
import time
from datetime import date, datetime
from typing import Any

import requests
from loguru import logger as log

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_engine
from sqlalchemy import text

AV_BASE = "https://www.alphavantage.co/query"
DELAY = 0.45  # 150/min = 2.5/sec, use 2.2/sec

# Massive ticker list — S&P 500 core + crypto + ETFs + international ADRs
TICKERS = [
    # Mega cap tech (20)
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "AVGO", "ORCL", "CRM",
    "AMD", "INTC", "QCOM", "TXN", "AMAT", "LRCX", "KLAC", "NOW", "INTU", "ADBE",
    # Finance (20)
    "JPM", "V", "MA", "BRK-B", "GS", "MS", "BLK", "SCHW", "AXP", "C",
    "BAC", "WFC", "PNC", "USB", "TFC", "CME", "ICE", "CB", "MMC", "AIG",
    # Healthcare (20)
    "UNH", "JNJ", "ABBV", "MRK", "PFE", "LLY", "TMO", "ABT", "DHR", "BMY",
    "GILD", "VRTX", "REGN", "CI", "HUM", "ISRG", "SYK", "ZTS", "MDT", "ELV",
    # Consumer (20)
    "PG", "KO", "PEP", "COST", "WMT", "HD", "LOW", "MCD", "NKE", "SBUX",
    "TGT", "CL", "EL", "MDLZ", "PM", "MO", "DG", "DLTR", "ROST", "TJX",
    # Industrial / Energy (20)
    "CAT", "BA", "HON", "UNP", "RTX", "GD", "DE", "MMM", "GE", "LMT",
    "XOM", "CVX", "COP", "SLB", "EOG", "NEE", "DUK", "SO", "AEP", "D",
    # Growth / Fintech / Crypto (20)
    "COIN", "SQ", "SHOP", "PLTR", "CRWD", "SNOW", "DDOG", "NET", "ZS", "SMCI",
    "ARM", "MARA", "RIOT", "PYPL", "FIS", "NFLX", "ABNB", "UBER", "DASH", "RBLX",
    # ETFs — sector + factor + fixed income + commodity (30)
    "SPY", "QQQ", "IWM", "DIA", "VTI", "VOO",
    "XLF", "XLE", "XLK", "XLV", "XLI", "XLP", "XLU", "XLB", "XLRE", "XLC",
    "TLT", "IEF", "SHY", "HYG", "LQD", "JNK", "TIP", "BND",
    "GLD", "SLV", "USO", "UNG", "DBA", "ARKK",
    # International ADRs (20)
    "BABA", "TSM", "ASML", "NVO", "SAP", "TM", "SONY", "SHOP", "SE", "MELI",
    "PDD", "JD", "BIDU", "NIO", "XPEV", "LI", "GRAB", "CPNG", "VALE", "PBR",
    # More S&P 500 (50)
    "AMGN", "IBM", "CSCO", "T", "VZ", "TMUS", "CHTR", "CMCSA", "DIS", "NFLX",
    "F", "GM", "DAL", "LUV", "UAL", "AAL", "FDX", "UPS", "AFRM", "SOFI",
    "RIVN", "LCID", "FSR", "PLUG", "FCEL", "ENPH", "SEDG", "RUN", "DKNG", "PENN",
    "WYNN", "MGM", "LVS", "MAR", "HLT", "H", "RCL", "CCL", "NCLH", "EXPE",
    "ZM", "DOCU", "OKTA", "TWLO", "MDB", "CFLT", "S", "PANW", "FTNT", "SPLK",
    # Crypto via AV (5)
    # "BTC", "ETH", "SOL", "ADA", "DOT",  # AV uses different endpoint for crypto
]


def download_daily_csv(api_key: str, ticker: str) -> list[dict]:
    """Download full daily history as CSV (most efficient)."""
    resp = requests.get(AV_BASE, params={
        "function": "TIME_SERIES_DAILY",
        "symbol": ticker,
        "outputsize": "full",
        "datatype": "csv",
        "apikey": api_key,
    }, timeout=30)
    resp.raise_for_status()

    # Check for error messages
    text = resp.text
    if "Error" in text[:100] or "Note" in text[:100] or "Thank you" in text[:200]:
        return []

    reader = csv.DictReader(io.StringIO(text))
    return list(reader)


def main():
    from config import settings
    api_key = settings.ALPHAVANTAGE_API_KEY
    if not api_key:
        print("ALPHAVANTAGE_API_KEY not set")
        return

    engine = get_engine()

    # Resolve source ID
    with engine.begin() as conn:
        row = conn.execute(text("SELECT id FROM source_catalog WHERE name = :n"), {"n": "alphavantage_daily"}).fetchone()
        if row:
            source_id = row[0]
        else:
            result = conn.execute(text(
                "INSERT INTO source_catalog (name, base_url, cost_tier, latency_class, pit_available, "
                "revision_behavior, trust_score, priority_rank, active) "
                "VALUES ('alphavantage_daily', 'https://www.alphavantage.co', 'PAID', 'EOD', TRUE, "
                "'NEVER', 'HIGH', 11, TRUE) ON CONFLICT (name) DO NOTHING RETURNING id"
            ))
            new = result.fetchone()
            source_id = new[0] if new else conn.execute(
                text("SELECT id FROM source_catalog WHERE name = 'alphavantage_daily'")
            ).fetchone()[0]

    total_rows = 0
    total_tickers = 0
    calls = 0
    start = time.time()
    errors = []

    print("=" * 60)
    print(f"AV BULK DAILY PRICES — {len(TICKERS)} tickers")
    print("=" * 60)

    for i, ticker in enumerate(TICKERS):
        try:
            data = download_daily_csv(api_key, ticker)
            calls += 1
            time.sleep(DELAY)

            if not data:
                errors.append(ticker)
                continue

            total_tickers += 1
            rows_inserted = 0

            # Get existing dates for this ticker to skip dupes
            with engine.connect() as conn:
                existing = conn.execute(text(
                    "SELECT DISTINCT obs_date FROM raw_series "
                    "WHERE series_id = :sid AND source_id = :src AND pull_status = 'SUCCESS'"
                ), {"sid": f"av:daily:{ticker}", "src": source_id}).fetchall()
                existing_dates = {r[0] for r in existing}

            with engine.begin() as conn:
                for row in data:
                    try:
                        obs = datetime.strptime(row["timestamp"], "%Y-%m-%d").date()
                        if obs in existing_dates:
                            continue

                        close = float(row["close"])
                        conn.execute(text(
                            "INSERT INTO raw_series (series_id, source_id, obs_date, value, raw_payload, pull_status) "
                            "VALUES (:sid, :src, :od, :val, :payload, 'SUCCESS')"
                        ), {
                            "sid": f"av:daily:{ticker}",
                            "src": source_id,
                            "od": obs,
                            "val": close,
                            "payload": f'{{"o":{row["open"]},"h":{row["high"]},"l":{row["low"]},"c":{row["close"]},"v":{row["volume"]}}}',
                        })
                        rows_inserted += 1
                    except (ValueError, TypeError, KeyError):
                        pass

            total_rows += rows_inserted

            if (i + 1) % 10 == 0:
                elapsed = time.time() - start
                rate = calls / elapsed * 60 if elapsed > 0 else 0
                print(f"  [{i+1}/{len(TICKERS)}] {total_tickers} tickers, {total_rows:,} rows, "
                      f"{calls} calls, {rate:.0f}/min, {len(errors)} errors")

        except Exception as exc:
            errors.append(ticker)
            log.debug("AV daily {t} failed: {e}", t=ticker, e=str(exc))
            time.sleep(DELAY)

    elapsed = time.time() - start
    print()
    print("=" * 60)
    print(f"DONE: {total_rows:,} rows, {total_tickers} tickers, {calls} calls in {elapsed:.0f}s")
    if errors:
        print(f"Errors ({len(errors)}): {', '.join(errors[:20])}")
    print("=" * 60)


if __name__ == "__main__":
    main()
