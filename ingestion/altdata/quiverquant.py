"""GRID — QuiverQuant Expanded Puller.

Pulls all available endpoints from QuiverQuant API (Trader plan $75/mo):
  - WSB Mentions (wallstreetbets sentiment)
  - Wikipedia Trends (attention proxy)
  - Government Contracts (fiscal flow signal)
  - Lobbying (corporate influence signal)
  - Senate/House Trading (congressional insider proxy)
  - Insider Trading (QQ cleaned version)
  - Patent Filings (innovation signal)
  - SPAC Deals
  - Political Beta (party correlation)

All data stored in signal_sources with source_type='quiverquant:{endpoint}'.
"""

from __future__ import annotations

import os
import time
from datetime import date, datetime, timedelta
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


_BASE_URL = "https://api.quiverquant.com/beta"
_RATE_LIMIT = 1.0  # seconds between requests
_TIMEOUT = 30

# Endpoints to pull with their config
ENDPOINTS = {
    "wsb": {
        "path": "/live/wallstreetbets",
        "source_type": "quiverquant:wsb",
        "description": "WallStreetBets ticker mentions and sentiment",
    },
    "lobbying": {
        "path": "/live/lobbying",
        "source_type": "quiverquant:lobbying",
        "description": "Corporate lobbying expenditures",
    },
    "insider_trading": {
        "path": "/live/insiders",
        "source_type": "quiverquant:insider",
        "description": "Insider trading filings (QQ cleaned)",
    },
    "gov_contracts": {
        "path": "/live/govcontracts",
        "source_type": "quiverquant:gov_contracts",
        "description": "Federal government contracts by ticker/quarter",
    },
    "off_exchange": {
        "path": "/live/offexchange",
        "source_type": "quiverquant:offexchange",
        "description": "Dark pool / OTC short volume with DPI",
    },
    "flights": {
        "path": "/live/flights",
        "source_type": "quiverquant:flights",
        "description": "Corporate jet tracking (departure/arrival cities)",
    },
    "senate_trading": {
        "path": "/live/senatetrading",
        "source_type": "quiverquant:senate",
        "description": "Senate stock trading disclosures",
    },
    "house_trading": {
        "path": "/live/housetrading",
        "source_type": "quiverquant:house",
        "description": "House stock trading disclosures",
    },
    "twitter": {
        "path": "/live/twitter",
        "source_type": "quiverquant:twitter",
        "description": "Twitter follower changes for companies",
    },
    "political_beta": {
        "path": "/live/politicalbeta",
        "source_type": "quiverquant:political_beta",
        "description": "Stock correlation with political outcomes (Trump beta)",
    },
}


def _get_api_key() -> str:
    """Get QuiverQuant API key from environment."""
    key = os.environ.get("QUIVERQUANT_API_KEY", "")
    if not key:
        raise ValueError("QUIVERQUANT_API_KEY not set in environment")
    return key


def _fetch_endpoint(path: str, api_key: str) -> list[dict]:
    """Fetch data from a QuiverQuant endpoint."""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }
    url = f"{_BASE_URL}{path}"
    resp = requests.get(url, headers=headers, timeout=_TIMEOUT)

    if resp.status_code == 429:
        log.warning("QuiverQuant rate limited on {}", path)
        time.sleep(5)
        resp = requests.get(url, headers=headers, timeout=_TIMEOUT)

    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else []


def _store_signals(
    engine: Engine,
    records: list[dict],
    source_type: str,
    endpoint_key: str,
) -> int:
    """Store QuiverQuant records into signal_sources table."""
    if not records:
        return 0

    import json as _json

    rows_inserted = 0
    today = date.today()

    with engine.begin() as conn:
        for rec in records:
            ticker = rec.get("Ticker") or rec.get("ticker") or ""
            if not ticker:
                continue

            # Parse date — many QQ endpoints return current-day data without a date
            date_str = rec.get("Date") or rec.get("date") or rec.get("ReportDate") or ""
            if date_str:
                try:
                    if isinstance(date_str, str):
                        signal_date = datetime.fromisoformat(date_str.replace("Z", "+00:00")).date()
                    elif isinstance(date_str, (int, float)):
                        signal_date = datetime.fromtimestamp(date_str / 1000).date()
                    else:
                        signal_date = date_str
                except (ValueError, TypeError):
                    signal_date = today
            else:
                signal_date = today

            # Build signal_value as proper JSON
            signal_value = {k: v for k, v in rec.items()
                           if k not in ("Ticker", "ticker", "Date", "date", "ReportDate")}

            # Determine signal_type from endpoint
            signal_type = endpoint_key
            if endpoint_key == "wsb":
                sentiment = rec.get("Sentiment", 0)
                signal_type = "wsb_bullish" if sentiment and sentiment > 0 else "wsb_bearish" if sentiment and sentiment < 0 else "wsb_neutral"
            elif endpoint_key == "insider_trading":
                txn = rec.get("TransactionType", "")
                signal_type = "insider_buy" if "buy" in str(txn).lower() else "insider_sell"

            try:
                conn.execute(text("""
                    INSERT INTO signal_sources
                        (source_type, source_id, signal_type, ticker, signal_date, signal_value, created_at)
                    VALUES
                        (:source_type, :source_id, :signal_type, :ticker, :signal_date, CAST(:signal_value AS jsonb), NOW())
                    ON CONFLICT (source_type, source_id, ticker, signal_date, signal_type)
                    DO UPDATE SET signal_value = EXCLUDED.signal_value
                """), {
                    "source_type": source_type,
                    "source_id": f"qq_{endpoint_key}",
                    "signal_type": signal_type,
                    "ticker": ticker.upper(),
                    "signal_date": signal_date,
                    "signal_value": _json.dumps(signal_value),
                })
                rows_inserted += 1
            except Exception as exc:
                log.debug("QuiverQuant insert skip for {}: {}", ticker, exc)

    return rows_inserted


def pull_endpoint(
    engine: Engine,
    endpoint_key: str,
) -> dict[str, Any]:
    """Pull a single QuiverQuant endpoint."""
    if endpoint_key not in ENDPOINTS:
        return {"endpoint": endpoint_key, "status": "UNKNOWN", "rows": 0}

    cfg = ENDPOINTS[endpoint_key]
    api_key = _get_api_key()

    log.info("QuiverQuant pulling: {} ({})", endpoint_key, cfg["description"])

    try:
        records = _fetch_endpoint(cfg["path"], api_key)
        rows = _store_signals(engine, records, cfg["source_type"], endpoint_key)
        log.info("QuiverQuant {}: {} records fetched, {} stored", endpoint_key, len(records), rows)
        time.sleep(_RATE_LIMIT)
        return {"endpoint": endpoint_key, "status": "SUCCESS", "fetched": len(records), "stored": rows}
    except Exception as exc:
        log.error("QuiverQuant {} failed: {}", endpoint_key, exc)
        return {"endpoint": endpoint_key, "status": "FAILED", "error": str(exc)}


def pull_all(engine: Engine) -> list[dict[str, Any]]:
    """Pull all QuiverQuant endpoints."""
    log.info("QuiverQuant: pulling all {} endpoints", len(ENDPOINTS))
    results = []
    for key in ENDPOINTS:
        result = pull_endpoint(engine, key)
        results.append(result)
    total = sum(r.get("stored", 0) for r in results)
    ok = sum(1 for r in results if r["status"] == "SUCCESS")
    log.info("QuiverQuant: {}/{} endpoints succeeded, {} total rows stored", ok, len(ENDPOINTS), total)
    return results
