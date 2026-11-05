#!/usr/bin/env python3
"""Generate crypto predictions for 24/7 scoring.

Crypto markets never close — predictions can be scored immediately.
This script generates swing (7-day) and macro (30-day) predictions
for BTC, ETH, SOL and queues them for AstroGrid scoring.

Exit status is non-zero unless every generated prediction was persisted,
so the systemd unit reports a partial or empty run as a failure.
"""
from __future__ import annotations

import hashlib
import os
import sys
from datetime import datetime, timezone
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from store.astrogrid import AstroGridStore

MODEL_VERSION = "grid-crypto-v1"
WEIGHT_VERSION = "astrogrid-v1"


def load_market_inputs(engine: Engine) -> tuple[dict[str, float], Any]:
    """Return the latest resolved BTC/ETH/SOL prices and the latest thesis row."""
    prices: dict[str, float] = {}
    with engine.connect() as conn:
        for ticker in ["BTC", "ETH", "SOL"]:
            row = conn.execute(text(
                "SELECT value FROM resolved_series "
                "WHERE feature_id = (SELECT id FROM feature_registry WHERE name = :f) "
                "ORDER BY obs_date DESC LIMIT 1"
            ), {"f": f"{ticker.lower()}_full"}).fetchone()
            if row:
                prices[ticker] = float(row[0])

        thesis_row = conn.execute(text(
            "SELECT overall_direction, conviction FROM thesis_snapshots ORDER BY timestamp DESC LIMIT 1"
        )).fetchone()
    return prices, thesis_row


def build_calls(prices: dict[str, float], now: datetime) -> list[dict[str, str]]:
    """Crypto predictions — mix of swing and macro, bullish/bearish/neutral."""
    btc = prices.get('BTC', 70000)
    eth = prices.get('ETH', 2100)
    sol = prices.get('SOL', 90)

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

        # INTRADAY / SAME-DAY predictions (scoreable within hours)
        {"symbol": "BTC", "horizon": "swing", "direction": "bullish",
         "call": f"BTC holds above ${btc*0.99:.0f} for next 4 hours",
         "timing": f"{now.strftime('%Y-%m-%d %H:%M')} -> 4h",
         "setup": f"BTC at ${btc:.0f}, low vol weekend, buyers defending level",
         "invalidation": f"Drops below ${btc*0.99:.0f}"},
        {"symbol": "BTC", "horizon": "swing", "direction": "bearish",
         "call": f"BTC dips to ${btc*0.97:.0f} within 12 hours",
         "timing": f"{now.strftime('%Y-%m-%d %H:%M')} -> 12h",
         "setup": "Thin weekend liquidity, funding positive, leverage flush due",
         "invalidation": f"New high above ${btc*1.01:.0f}"},
        {"symbol": "BTC", "horizon": "swing", "direction": "neutral",
         "call": f"BTC stays in ${btc*0.98:.0f}-${btc*1.02:.0f} range for 24h",
         "timing": f"{now.strftime('%Y-%m-%d %H:%M')} -> 24h",
         "setup": "Weekend consolidation, no catalyst expected",
         "invalidation": "Breaks 2% in either direction"},
        {"symbol": "ETH", "horizon": "swing", "direction": "bearish",
         "call": "ETH/BTC ratio continues declining today",
         "timing": f"{now.strftime('%Y-%m-%d %H:%M')} -> 24h",
         "setup": "ETH underperforming BTC for weeks, no reversal catalyst",
         "invalidation": "ETH outperforms BTC by 1%+ today"},
        {"symbol": "ETH", "horizon": "swing", "direction": "bullish",
         "call": f"ETH bounces from ${eth*0.98:.0f} if tested",
         "timing": f"{now.strftime('%Y-%m-%d %H:%M')} -> 12h",
         "setup": f"ETH at ${eth:.0f}, oversold on 4h RSI historically",
         "invalidation": f"Clean break below ${eth*0.97:.0f}"},
        {"symbol": "SOL", "horizon": "swing", "direction": "bullish",
         "call": f"SOL holds ${sol*0.97:.0f} support into tomorrow",
         "timing": f"{now.strftime('%Y-%m-%d %H:%M')} -> 24h",
         "setup": f"SOL at ${sol:.0f}, memecoin volume still elevated on Raydium",
         "invalidation": f"Breaks below ${sol*0.95:.0f}"},
        {"symbol": "SOL", "horizon": "swing", "direction": "bearish",
         "call": f"SOL gives back gains, tests ${sol*0.93:.0f}",
         "timing": f"{now.strftime('%Y-%m-%d %H:%M')} -> 24h",
         "setup": "Memecoin hype fading, profit-taking after weekly rally",
         "invalidation": f"New local high above ${sol*1.05:.0f}"},
        {"symbol": "BTC", "horizon": "swing", "direction": "bullish",
         "call": f"BTC closes today above ${btc:.0f}",
         "timing": f"{now.strftime('%Y-%m-%d %H:%M')} -> end of UTC day",
         "setup": "Buyers defending round number, accumulation zone",
         "invalidation": f"Closes below ${btc*0.98:.0f}"},
    ]
    return calls


def prediction_id_for(call: dict[str, str], now: datetime) -> str:
    """Deterministic id for one call of one run.

    Several calls in a run share symbol/horizon/direction (e.g. three
    BTC swing bullish calls), so the call text and timing are part of the
    key. Before 2026-11-05 they were not, and 6 of 16 calls collided on
    ``prediction_run_prediction_id_key`` every run.
    """
    key = "|".join(
        (call["symbol"], call["horizon"], call["direction"], call["timing"], call["call"], now.isoformat())
    )
    return hashlib.sha256(key.encode()).hexdigest()[:36]


def build_payloads(
    calls: list[dict[str, str]],
    prices: dict[str, float],
    thesis_row: Any,
    now: datetime,
) -> list[dict[str, Any]]:
    """Build ``AstroGridStore.save_prediction`` payloads (native dicts/lists, not JSON strings)."""
    payloads: list[dict[str, Any]] = []
    for c in calls:
        payloads.append({
            "prediction_id": prediction_id_for(c, now),
            "as_of_ts": now.isoformat(),
            "horizon_label": c["horizon"],
            "target_universe": "hybrid",
            "scoring_class": "liquid_market",
            "target_symbols": [c["symbol"]],
            "question": f"What will {c['symbol']} do?",
            "call": c["call"],
            "timing": c["timing"],
            "setup": c["setup"],
            "invalidation": c["invalidation"],
            "note": "Auto-generated crypto prediction for 24/7 scoring",
            "seer_summary": "",
            "market_overlay_snapshot": {"prices": dict(prices)},
            "mystical_feature_payload": {},
            "grid_feature_payload": {
                "thesis": thesis_row[0] if thesis_row else "NEUTRAL",
                "conviction": float(thesis_row[1]) if thesis_row else 0,
            },
            "weight_version": WEIGHT_VERSION,
            "model_version": MODEL_VERSION,
            "live_or_local": "live",
            "status": "pending",
        })
    ids = [p["prediction_id"] for p in payloads]
    if len(set(ids)) != len(ids):
        raise ValueError("generate_crypto_predictions: duplicate prediction_id within one run")
    return payloads


def persist_payloads(store: AstroGridStore, payloads: list[dict[str, Any]]) -> int:
    """Insert each payload once (never updates existing rows); return how many were stored."""
    stored = 0
    for payload in payloads:
        result = store.save_prediction(payload)
        symbol = payload["target_symbols"][0]
        if result:
            log.info("  {} {:5s} — {}", symbol, payload["horizon_label"], payload["call"][:50])
            stored += 1
        else:
            # save_prediction already logged the persistence failure at WARNING.
            log.warning("  prediction not stored: {} {}", symbol, payload["call"][:40])
    return stored


def main() -> int:
    from db import get_engine

    engine = get_engine()
    store = AstroGridStore(engine)

    # Ensure weight version
    wv = store.ensure_active_weight_version()
    log.info("Weight version: {}", wv.get('version_key', '?'))

    prices, thesis_row = load_market_inputs(engine)
    log.info("Prices: {}", prices)
    log.info("Thesis: {}", thesis_row[0] if thesis_row else 'N/A')

    now = datetime.now(timezone.utc)
    payloads = build_payloads(build_calls(prices, now), prices, thesis_row, now)
    stored = persist_payloads(store, payloads)
    log.info("Stored {} of {} crypto predictions", stored, len(payloads))

    with engine.connect() as conn:
        r = conn.execute(text("SELECT COUNT(*) FROM astrogrid.prediction_run")).fetchone()
        pending = conn.execute(text("SELECT COUNT(*) FROM astrogrid.prediction_run WHERE status = 'pending'")).fetchone()
        log.info("Total predictions: {} ({} pending scoring)", r[0], pending[0])

    if stored != len(payloads):
        log.error(
            "generate_crypto_predictions: persisted {} of {} predictions; failing the run",
            stored, len(payloads),
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
