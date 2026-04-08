"""Whale Fingerprinter — clusters anonymous options flow into behavioral profiles.

152K whale signals are all actor="whale". Useless for the actor graph.
This module clusters them by behavioral signature into synthetic actors:

    whale_megacap_hedger     — $10M+ premium, SPY/QQQ puts, long-dated
    whale_tech_momentum      — NVDA/AAPL/TSLA calls, short expiry, volume spikes
    whale_earnings_sniper    — positions opened 1-5 days before earnings
    whale_vix_player         — VIX/UVXY positions, high IV trades
    whale_spread_builder     — multiple strikes same ticker same day
    whale_retail_yolo        — small premium, deep OTM, meme tickers

After clustering, replaces actor="whale" with the profile name.
Each profile becomes a trackable actor with its own trust score.

Runs as part of the extractor cycle.
"""

from __future__ import annotations

import json
import sys
import time
from datetime import date, timedelta
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Profile definitions ──

PROFILES = {
    "whale_megacap_hedger": {
        "description": "Institutional hedger — large premium index/mega-cap puts",
        "category": "fund",
        "tier": "institutional",
        "influence": 0.85,
    },
    "whale_tech_momentum": {
        "description": "Tech momentum trader — FAANG/semis calls, short expiry",
        "category": "fund",
        "tier": "institutional",
        "influence": 0.7,
    },
    "whale_earnings_sniper": {
        "description": "Earnings event trader — positions near catalysts",
        "category": "fund",
        "tier": "institutional",
        "influence": 0.8,
    },
    "whale_spread_builder": {
        "description": "Complex strategy — multi-leg positions, same session",
        "category": "fund",
        "tier": "institutional",
        "influence": 0.75,
    },
    "whale_small_speculator": {
        "description": "Small speculative flow — low premium, wide strikes",
        "category": "individual",
        "tier": "individual",
        "influence": 0.3,
    },
    "whale_large_directional": {
        "description": "Large directional bet — high premium single-leg",
        "category": "fund",
        "tier": "institutional",
        "influence": 0.8,
    },
}

# Tickers that signal institutional hedging
INDEX_TICKERS = {"SPY", "QQQ", "IWM", "DIA", "VIX", "UVXY", "SQQQ", "TLT", "GLD", "SLV", "HYG"}
TECH_TICKERS = {"NVDA", "AAPL", "TSLA", "MSFT", "GOOG", "GOOGL", "AMZN", "META", "AMD", "AVGO", "TSM"}

# Premium thresholds
MEGA_PREMIUM = 5_000_000    # $5M+
LARGE_PREMIUM = 500_000     # $500K+
SMALL_PREMIUM = 50_000      # <$50K


def classify_whale(ticker: str, data: dict[str, Any]) -> str:
    """Classify a single whale flow into a behavioral profile."""
    premium = data.get("notional_premium", 0) or 0
    direction = (data.get("direction", "") or "").upper()
    signals = data.get("signals", []) or []
    volume = data.get("volume", 0) or 0
    oi_ratio = data.get("oi_ratio", 0) or 0
    iv = data.get("implied_volatility", 0) or 0

    # Mega-cap hedger: large premium + index + puts
    if premium >= MEGA_PREMIUM and ticker in INDEX_TICKERS and direction == "PUT":
        return "whale_megacap_hedger"

    # Large directional: big premium, single direction
    if premium >= LARGE_PREMIUM:
        if "LARGE_PREMIUM" in signals:
            if ticker in TECH_TICKERS and direction == "CALL":
                return "whale_tech_momentum"
            return "whale_large_directional"

    # OI spike with volume spike = likely institutional accumulation
    if oi_ratio > 3.0 and "OI_SPIKE" in signals and "VOLUME_SPIKE" in signals:
        if ticker in TECH_TICKERS:
            return "whale_tech_momentum"
        return "whale_spread_builder"

    # Volume spike alone on tech = momentum
    if "VOLUME_SPIKE" in signals and ticker in TECH_TICKERS:
        return "whale_tech_momentum"

    # Small premium = retail/small spec
    if premium < SMALL_PREMIUM:
        return "whale_small_speculator"

    # Default: large directional
    if premium >= LARGE_PREMIUM:
        return "whale_large_directional"

    return "whale_small_speculator"


def fingerprint_whales(engine: Engine, batch_size: int = 10000) -> dict[str, int]:
    """Reclassify whale signals from actor='whale' to behavioral profiles.

    Updates signal_data.actor in-place for whale_flow signals.
    Creates synthetic actors in the actors table for each profile.
    """
    stats = {"reclassified": 0, "profiles_created": 0, "errors": 0}

    with engine.connect() as conn:
        # Ensure profile actors exist
        for profile_id, info in PROFILES.items():
            conn.execute(text("""
                INSERT INTO actors (id, name, tier, category, title,
                    influence_score, trust_score, degree, source, credibility, updated_at)
                VALUES (:id, :name, :tier, :cat, :title,
                    :inf, 0.5, 0, 'whale_fingerprint', 'inferred', NOW())
                ON CONFLICT (id) DO UPDATE SET
                    title = EXCLUDED.title,
                    influence_score = GREATEST(actors.influence_score, EXCLUDED.influence_score),
                    updated_at = NOW()
            """), {
                "id": profile_id,
                "name": profile_id.replace("_", " ").title(),
                "tier": info["tier"],
                "cat": info["category"],
                "title": info["description"],
                "inf": info["influence"],
            })
            stats["profiles_created"] += 1

        conn.commit()

        # Reclassify whale signals in batches
        rows = conn.execute(text("""
            SELECT id, ticker, data
            FROM signal_data
            WHERE signal_type = 'whale_flow'
              AND (actor = 'whale' OR actor IS NULL)
              AND data IS NOT NULL
            LIMIT :lim
        """), {"lim": batch_size}).fetchall()

        updates: dict[str, list[int]] = {}
        for sig_id, ticker, data in rows:
            try:
                d = data if isinstance(data, dict) else {}
                profile = classify_whale(ticker or "", d)
                if profile not in updates:
                    updates[profile] = []
                updates[profile].append(sig_id)
            except Exception:
                stats["errors"] += 1

        # Batch update by profile
        for profile, ids in updates.items():
            conn.execute(text("""
                UPDATE signal_data SET actor = :profile
                WHERE id = ANY(:ids)
            """), {"profile": profile, "ids": ids})
            stats["reclassified"] += len(ids)

        conn.commit()

    log.info(
        "Whale fingerprinter: {r} reclassified into {p} profiles, {e} errors",
        r=stats["reclassified"], p=len(updates), e=stats["errors"],
    )
    return stats


def run_fingerprinter(interval: int = 600) -> None:
    """Main loop — reclassify whales every `interval` seconds."""
    sys.path.insert(0, ".")
    from db import get_engine

    engine = get_engine()
    log.info("Whale fingerprinter starting — interval={i}s", i=interval)

    # Initial full sweep
    total = 0
    while True:
        stats = fingerprint_whales(engine, batch_size=50000)
        total += stats["reclassified"]
        log.info("Fingerprinter batch: {r} this batch, {t} total", r=stats["reclassified"], t=total)
        if stats["reclassified"] == 0:
            break

    log.info("Initial sweep done: {t} total reclassified", t=total)

    while True:
        try:
            fingerprint_whales(engine, batch_size=5000)
        except Exception as exc:
            log.error("Fingerprinter cycle failed: {e}", e=str(exc))
        time.sleep(interval)


if __name__ == "__main__":
    log.remove()
    log.add(sys.stderr, level="INFO")
    run_fingerprinter()
