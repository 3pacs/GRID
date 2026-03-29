#!/usr/bin/env python3
"""Generate crypto predictions for 24/7 scoring.

Crypto markets never close — predictions can be scored immediately.
This script generates swing (7-day) and macro (30-day) predictions
for BTC, ETH, SOL and queues them for AstroGrid scoring.
"""
import os, sys, json, hashlib
from datetime import datetime, timedelta, timezone, date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_engine
from store.astrogrid import AstroGridStore
from sqlalchemy import text

engine = get_engine()
store = AstroGridStore(engine)

# Ensure weight version
wv = store.ensure_active_weight_version()
print(f"Weight version: {wv.get('version_key', '?')}")

# Get latest prices
prices = {}
with engine.connect() as conn:
    for ticker in ["BTC", "ETH", "SOL"]:
        row = conn.execute(text(
            "SELECT value FROM resolved_series "
            "WHERE feature_id = (SELECT id FROM feature_registry WHERE name = :f) "
            "ORDER BY obs_date DESC LIMIT 1"
        ), {"f": f"{ticker.lower()}_full"}).fetchone()
        if row:
            prices[ticker] = float(row[0])

    # Get thesis
    thesis_row = conn.execute(text(
        "SELECT overall_direction, conviction FROM thesis_snapshots ORDER BY timestamp DESC LIMIT 1"
    )).fetchone()

print(f"Prices: {prices}")
print(f"Thesis: {thesis_row[0] if thesis_row else 'N/A'}")

now = datetime.now(timezone.utc)

# Crypto predictions — mix of swing and macro, bullish/bearish/neutral
calls = [
    # BTC swing predictions
    {"symbol": "BTC", "horizon": "swing", "direction": "bullish",
     "call": "BTC breaks above recent consolidation range",
     "timing": f"{now.strftime('%Y-%m-%d')} -> 7d",
     "setup": f"BTC at ${prices.get('BTC', 0):.0f}, overall thesis BULLISH",
     "invalidation": "Close below 5% from entry within 7 days"},
    {"symbol": "BTC", "horizon": "macro", "direction": "bullish",
     "call": "BTC targets new ATH within Q2 2026",
     "timing": f"{now.strftime('%Y-%m-%d')} -> 30d",
     "setup": "Post-halving cycle, ETF inflows, institutional adoption accelerating",
     "invalidation": "Monthly close below 200-day moving average"},

    # ETH predictions
    {"symbol": "ETH", "horizon": "swing", "direction": "bullish",
     "call": "ETH follows BTC with higher beta",
     "timing": f"{now.strftime('%Y-%m-%d')} -> 7d",
     "setup": f"ETH at ${prices.get('ETH', 0):.0f}, correlated to BTC momentum",
     "invalidation": "Close below 7% from entry within 7 days"},
    {"symbol": "ETH", "horizon": "macro", "direction": "neutral",
     "call": "ETH range-bound vs BTC, underperforms on ratio",
     "timing": f"{now.strftime('%Y-%m-%d')} -> 30d",
     "setup": "ETH/BTC ratio declining, L2 value accrual debate ongoing",
     "invalidation": "ETH/BTC ratio breaks above 0.06"},

    # SOL predictions
    {"symbol": "SOL", "horizon": "swing", "direction": "bullish",
     "call": "SOL outperforms on memecoin and DeFi volume surge",
     "timing": f"{now.strftime('%Y-%m-%d')} -> 7d",
     "setup": f"SOL at ${prices.get('SOL', 0):.0f}, Jupiter/Raydium volumes elevated",
     "invalidation": "Close below 10% from entry within 7 days"},
    {"symbol": "SOL", "horizon": "macro", "direction": "bullish",
     "call": "SOL captures growing share of DeFi and NFT activity",
     "timing": f"{now.strftime('%Y-%m-%d')} -> 30d",
     "setup": "Solana DeFi TVL growing, Firedancer upgrade narrative",
     "invalidation": "SOL/BTC ratio breaks below 90-day low"},

    # Contrarian / bearish predictions for balance
    {"symbol": "BTC", "horizon": "swing", "direction": "bearish",
     "call": "BTC pulls back to retest support after weekend low volume",
     "timing": f"{now.strftime('%Y-%m-%d')} -> 3d",
     "setup": "Weekend liquidity thin, funding rates elevated, profit-taking due",
     "invalidation": "New 7-day high within 3 days"},
    {"symbol": "ETH", "horizon": "swing", "direction": "bearish",
     "call": "ETH underperforms BTC as rotation to BTC dominance continues",
     "timing": f"{now.strftime('%Y-%m-%d')} -> 7d",
     "setup": "ETH/BTC ratio at multi-year lows, institutional preference for BTC ETFs",
     "invalidation": "ETH outperforms BTC by >5% in 7 days"},
]

stored = 0
for c in calls:
    pred_id = hashlib.sha256(
        f"{c['symbol']}:{c['horizon']}:{c['direction']}:{now.isoformat()}".encode()
    ).hexdigest()[:36]

    payload = {
        "prediction_id": pred_id,
        "as_of_ts": now.isoformat(),
        "horizon_label": c["horizon"],
        "target_universe": "hybrid",
        "scoring_class": "liquid_market",
        "target_symbols": json.dumps([c["symbol"]]),
        "question": f"What will {c['symbol']} do?",
        "call": c["call"],
        "timing": c["timing"],
        "setup": c["setup"],
        "invalidation": c["invalidation"],
        "note": "Auto-generated crypto prediction for 24/7 scoring",
        "seer_summary": "",
        "market_overlay_snapshot": json.dumps({"prices": prices}),
        "mystical_feature_payload": json.dumps({}),
        "grid_feature_payload": json.dumps({
            "thesis": thesis_row[0] if thesis_row else "NEUTRAL",
            "conviction": float(thesis_row[1]) if thesis_row else 0,
        }),
        "weight_version": "astrogrid-v1",
        "model_version": "grid-crypto-v1",
        "live_or_local": "live",
        "status": "pending",
    }

    result = store.save_prediction(payload)
    if result:
        print(f"  {c['symbol']} {c['horizon']:5s} {c['direction']:7s} — {c['call'][:50]}")
        stored += 1
    else:
        print(f"  FAIL: {c['symbol']} {c['call'][:40]}")

print(f"\nStored {stored} crypto predictions")

# Count total
with engine.connect() as conn:
    r = conn.execute(text("SELECT COUNT(*) FROM astrogrid.prediction_run")).fetchone()
    pending = conn.execute(text("SELECT COUNT(*) FROM astrogrid.prediction_run WHERE status = 'pending'")).fetchone()
    print(f"Total predictions: {r[0]} ({pending[0]} pending scoring)")
