#!/usr/bin/env python3
"""Score crypto predictions against live prices via Crypto.com API.

Crypto markets are 24/7 — predictions can be scored immediately.
This script should run every hour via Hermes or cron.
"""
import os, sys, json, requests
from datetime import datetime, timezone
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_engine
from sqlalchemy import text

engine = get_engine()

# Fetch live prices from Crypto.com
def get_live_price(symbol):
    """Get live price from Crypto.com API."""
    ticker_map = {"BTC": "BTC_USDT", "ETH": "ETH_USDT", "SOL": "SOL_USDT"}
    instrument = ticker_map.get(symbol)
    if not instrument:
        return None
    try:
        resp = requests.get(
            f"https://api.crypto.com/exchange/v1/public/get-ticker?instrument_name={instrument}",
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json().get("result", {}).get("data", [{}])
        if isinstance(data, list) and data:
            return float(data[0].get("a", 0))  # last price
        elif isinstance(data, dict):
            return float(data.get("a", 0))
        return None
    except Exception:
        return None

# Get live prices
prices = {}
for sym in ["BTC", "ETH", "SOL"]:
    p = get_live_price(sym)
    if p:
        prices[sym] = p
        print(f"  {sym}: ${p:,.2f}")

if not prices:
    print("Failed to fetch live prices")
    sys.exit(1)

# Get pending predictions
with engine.connect() as conn:
    rows = conn.execute(text(
        "SELECT id, prediction_id, target_symbols, call, setup, "
        "market_overlay_snapshot, as_of_ts "
        "FROM astrogrid.prediction_run "
        "WHERE status = 'pending' "
        "AND target_symbols::text LIKE '%BTC%' "
        "OR target_symbols::text LIKE '%ETH%' "
        "OR target_symbols::text LIKE '%SOL%' "
        "ORDER BY as_of_ts DESC"
    )).fetchall()

print(f"\n{len(rows)} crypto predictions to evaluate")

scored = 0
for row in rows:
    pred_id = row[1]
    symbols = row[2] if isinstance(row[2], list) else json.loads(row[2] or "[]")
    call_text = row[3] or ""
    setup_text = row[4] or ""
    overlay = row[5] if isinstance(row[5], dict) else json.loads(row[5] or "{}")
    as_of = row[6]

    # Get the entry price from the overlay
    entry_prices = overlay.get("prices", {})

    for sym in symbols:
        if sym not in prices or sym not in entry_prices:
            continue

        entry = entry_prices[sym]
        current = prices[sym]
        pct_change = (current - entry) / entry

        # Determine if prediction was bullish, bearish, or neutral
        call_lower = call_text.lower()
        if "bullish" in call_lower or "breaks above" in call_lower or "holds" in call_lower or "bounces" in call_lower or "targets" in call_lower or "outperforms" in call_lower:
            direction = "bullish"
        elif "bearish" in call_lower or "pulls back" in call_lower or "dips" in call_lower or "underperforms" in call_lower or "gives back" in call_lower:
            direction = "bearish"
        else:
            direction = "neutral"

        # Score
        if direction == "bullish":
            verdict = "hit" if pct_change > 0.01 else ("miss" if pct_change < -0.02 else "partial")
        elif direction == "bearish":
            verdict = "hit" if pct_change < -0.01 else ("miss" if pct_change > 0.02 else "partial")
        else:  # neutral
            verdict = "hit" if abs(pct_change) < 0.02 else "miss"

        print(f"  {sym} {direction:7s} entry=${entry:,.0f} now=${current:,.0f} ({pct_change:+.1%}) → {verdict}")
        scored += 1

print(f"\nEvaluated {scored} predictions")
print(f"Note: Not updating DB status yet — run with --commit to persist scores")
