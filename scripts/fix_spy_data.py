#!/usr/bin/env python3
"""
Quick fix: pull latest SPY + core index prices via AlphaVantage.

Targets only the gap: fetches compact (100-day) history and inserts
any dates missing from raw_series.

Run: python scripts/fix_spy_data.py
"""
from __future__ import annotations

import csv
import io
import os
import sys
import time
from datetime import datetime

import requests
from loguru import logger as log

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import settings
from db import get_engine
from sqlalchemy import text

AV_BASE = "https://www.alphavantage.co/query"
DELAY = 0.5  # stay under 150/min

# Core tickers that need daily freshness
PRIORITY_TICKERS = [
    "SPY", "QQQ", "IWM", "DIA",
    "XLF", "XLE", "XLK", "XLV", "XLI", "XLP", "XLU", "XLB", "XLRE", "XLC",
    "TLT", "GLD", "HYG", "VIX",
]


def download_compact(api_key: str, ticker: str) -> list[dict]:
    """Download last 100 trading days as CSV."""
    resp = requests.get(AV_BASE, params={
        "function": "TIME_SERIES_DAILY",
        "symbol": ticker,
        "outputsize": "compact",
        "datatype": "csv",
        "apikey": api_key,
    }, timeout=30)
    resp.raise_for_status()
    body = resp.text
    if "Error" in body[:100] or "Note" in body[:100] or "Thank you" in body[:200]:
        log.warning("AV rate-limited or error for {t}: {msg}", t=ticker, msg=body[:120])
        return []
    return list(csv.DictReader(io.StringIO(body)))


def main():
    api_key = settings.ALPHAVANTAGE_API_KEY
    if not api_key:
        log.error("ALPHAVANTAGE_API_KEY not set")
        return

    engine = get_engine()

    # Resolve source_id
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT id FROM source_catalog WHERE name = :n"),
            {"n": "alphavantage_daily"},
        ).fetchone()
        if not row:
            log.error("source 'alphavantage_daily' not in source_catalog")
            return
        source_id = row[0]

    total_new = 0
    for ticker in PRIORITY_TICKERS:
        try:
            data = download_compact(api_key, ticker)
            time.sleep(DELAY)
            if not data:
                log.warning("No data for {t}", t=ticker)
                continue

            sid = f"av:daily:{ticker}"
            with engine.connect() as conn:
                existing = conn.execute(
                    text(
                        "SELECT DISTINCT obs_date FROM raw_series "
                        "WHERE series_id = :sid AND source_id = :src"
                    ),
                    {"sid": sid, "src": source_id},
                ).fetchall()
                existing_dates = {r[0] for r in existing}

            inserted = 0
            with engine.begin() as conn:
                for row in data:
                    try:
                        obs = datetime.strptime(row["timestamp"], "%Y-%m-%d").date()
                        if obs in existing_dates:
                            continue
                        close = float(row["close"])
                        conn.execute(
                            text(
                                "INSERT INTO raw_series "
                                "(series_id, source_id, obs_date, value, raw_payload, pull_status) "
                                "VALUES (:sid, :src, :od, :val, :payload, 'SUCCESS')"
                            ),
                            {
                                "sid": sid,
                                "src": source_id,
                                "od": obs,
                                "val": close,
                                "payload": (
                                    f'{{"o":{row["open"]},"h":{row["high"]},'
                                    f'"l":{row["low"]},"c":{row["close"]},"v":{row["volume"]}}}'
                                ),
                            },
                        )
                        inserted += 1
                    except (ValueError, TypeError, KeyError):
                        pass

            if inserted:
                log.info("{t}: +{n} new rows", t=ticker, n=inserted)
                total_new += inserted
            else:
                log.info("{t}: up to date", t=ticker)

        except Exception as exc:
            log.error("{t} failed: {e}", t=ticker, e=str(exc))

    log.info("Done — {n} new rows inserted across {t} tickers", n=total_new, t=len(PRIORITY_TICKERS))


if __name__ == "__main__":
    main()
