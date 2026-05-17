#!/usr/bin/env python3
"""
Populate signal_data table from existing database sources.

Transforms existing raw_series signals (WHALE, GOV_CONTRACT, LEGISLATION,
congressional, insider, dark_pool) and signal_sources (options_flow) into
the unified signal_data format that hypothesis_engine.py needs.

Run: python scripts/populate_signal_data.py
"""
from __future__ import annotations

import json
import os
import sys

from loguru import logger as log

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_engine
from sqlalchemy import text

# Shared INSERT statement for all signal_data inserts
_INSERT_SIGNAL = text("""
    INSERT INTO signal_data
        (signal_type, signal_date, ticker, actor, direction, signal_subtype,
         magnitude, description, data, confidence, source_id)
    VALUES (:st, :sd, :tk, :act, :dir, :sub, :mag, :desc,
            CAST(:data AS jsonb), :conf, :src)
""")


def _jsonify(payload) -> str:
    """Ensure payload is a JSON string, not a dict."""
    if payload is None:
        return "{}"
    if isinstance(payload, dict):
        return json.dumps(payload)
    if isinstance(payload, str):
        return payload
    return json.dumps(payload)


def populate_from_whale_flows(engine) -> int:
    """Convert WHALE:ticker:... raw_series entries into signal_data."""
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT series_id, obs_date, value, raw_payload
            FROM raw_series
            WHERE series_id LIKE 'WHALE:%'
            ORDER BY obs_date
        """)).fetchall()

        count = 0
        for r in rows:
            parts = r[0].split(":")
            ticker = parts[1] if len(parts) > 1 else None
            conn.execute(_INSERT_SIGNAL, {
                "st": "whale_flow",
                "sd": r[1],
                "tk": ticker,
                "act": "whale",
                "dir": "BULL" if r[2] > 0 else "BEAR",
                "sub": "whale_flow",
                "mag": abs(r[2]),
                "desc": f"Whale flow {ticker}: {r[2]:+.2f}",
                "data": _jsonify(r[3]),
                "conf": "derived",
                "src": "unusual_whales",
            })
            count += 1
        return count


def populate_from_gov_contracts(engine) -> int:
    """Convert GOV_CONTRACT raw_series entries into signal_data."""
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT series_id, obs_date, value, raw_payload
            FROM raw_series
            WHERE series_id LIKE 'GOV_CONTRACT:%'
            ORDER BY obs_date
        """)).fetchall()

        count = 0
        for r in rows:
            parts = r[0].split(":")
            ticker = parts[1] if len(parts) > 1 else None
            payload = r[3] if isinstance(r[3], dict) else (json.loads(r[3]) if r[3] else {})
            conn.execute(_INSERT_SIGNAL, {
                "st": "gov_contract",
                "sd": r[1],
                "tk": ticker,
                "act": payload.get("agency", "US Government"),
                "dir": None,
                "sub": "gov_contract",
                "mag": r[2],
                "desc": f"Gov contract {ticker}: ${r[2]:,.0f}",
                "data": _jsonify(r[3]),
                "conf": "confirmed",
                "src": "usaspending",
            })
            count += 1
        return count


def populate_from_legislation(engine) -> int:
    """Convert LEGISLATION raw_series entries into signal_data."""
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT series_id, obs_date, value, raw_payload
            FROM raw_series
            WHERE series_id LIKE 'LEGISLATION:%'
            ORDER BY obs_date
        """)).fetchall()

        count = 0
        for r in rows:
            parts = r[0].split(":")
            ticker = parts[1] if len(parts) > 1 else None
            payload = r[3] if isinstance(r[3], dict) else (json.loads(r[3]) if r[3] else {})
            conn.execute(_INSERT_SIGNAL, {
                "st": "legislation",
                "sd": r[1],
                "tk": ticker,
                "act": payload.get("sponsor", "Congress"),
                "dir": None,
                "sub": "legislation",
                "mag": r[2],
                "desc": f"Legislation affecting {ticker}",
                "data": _jsonify(r[3]),
                "conf": "confirmed",
                "src": "congress_gov",
            })
            count += 1
        return count


def populate_from_options_flow(engine) -> int:
    """Convert signal_sources options_flow entries into signal_data."""
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT source_type, signal_type, ticker, signal_date,
                   signal_value, trust_score
            FROM signal_sources
            WHERE source_type = 'options_flow'
            ORDER BY signal_date
        """)).fetchall()

        count = 0
        for r in rows:
            sv = r[4] or {}
            direction_raw = sv.get("direction", "neutral")
            direction = "BULL" if direction_raw == "CALL" else "BEAR" if direction_raw == "PUT" else "NEUTRAL"
            notional = sv.get("notional", 0)
            signals = sv.get("signals", [])

            conn.execute(_INSERT_SIGNAL, {
                "st": "unusual_options",
                "sd": r[3],
                "tk": r[2],
                "act": "options_market",
                "dir": direction,
                "sub": "unusual_options",
                "mag": notional,
                "desc": f"Unusual options {r[2]}: {direction_raw} ${notional:,.0f} ({', '.join(signals)})",
                "data": _jsonify(sv),
                "conf": "derived",
                "src": "options_scanner",
            })
            count += 1
        return count


def populate_from_social(engine) -> int:
    """Convert SOCIAL raw_series entries into signal_data."""
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT series_id, obs_date, value, raw_payload
            FROM raw_series
            WHERE series_id LIKE 'SOCIAL:%'
            ORDER BY obs_date
        """)).fetchall()

        count = 0
        for r in rows:
            parts = r[0].split(":")
            # SOCIAL:reddit:user:ticker:BEARISH
            ticker = parts[3] if len(parts) > 3 else None
            user = parts[2] if len(parts) > 2 else None
            sentiment = parts[4] if len(parts) > 4 else "neutral"

            conn.execute(_INSERT_SIGNAL, {
                "st": "social_sentiment",
                "sd": r[1],
                "tk": ticker,
                "act": user,
                "dir": "BULL" if sentiment == "BULLISH" else "BEAR" if sentiment == "BEARISH" else "NEUTRAL",
                "sub": "social_sentiment",
                "mag": abs(r[2]),
                "desc": f"Social {sentiment.lower()} on {ticker} by {user}",
                "data": _jsonify(r[3]),
                "conf": "estimated",
                "src": "reddit",
            })
            count += 1
        return count


def populate_from_congressional_raw(engine) -> int:
    """Convert qq:congress raw_series entries into signal_data."""
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT series_id, obs_date, value, raw_payload
            FROM raw_series
            WHERE series_id LIKE 'qq:congress:%'
            ORDER BY obs_date
        """)).fetchall()

        count = 0
        for r in rows:
            parts = r[0].split(":")
            # qq:congress:Name:Ticker
            actor = parts[2] if len(parts) > 2 else None
            ticker = parts[3] if len(parts) > 3 else None
            payload = r[3] if isinstance(r[3], dict) else (json.loads(r[3]) if r[3] else {})

            conn.execute(_INSERT_SIGNAL, {
                "st": "congressional_trade",
                "sd": r[1],
                "tk": ticker,
                "act": actor,
                "dir": ("BULL" if (payload.get("type", "buy") or "buy").lower() in ("buy","purchase") else "BEAR") if payload else "BULL",
                "sub": "congressional_trade",
                "mag": r[2],
                "desc": f"Congressional trade: {actor} on {ticker}",
                "data": _jsonify(r[3]),
                "conf": "confirmed",
                "src": "quiver_congress",
            })
            count += 1
        return count


def populate_from_lobbying(engine) -> int:
    """Convert qq:lobbying raw_series entries into signal_data."""
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT series_id, obs_date, value, raw_payload
            FROM raw_series
            WHERE series_id LIKE 'qq:lobbying:%'
            ORDER BY obs_date
        """)).fetchall()

        count = 0
        for r in rows:
            parts = r[0].split(":")
            entity = parts[2] if len(parts) > 2 else None
            r[3] if isinstance(r[3], dict) else (json.loads(r[3]) if r[3] else {})

            conn.execute(_INSERT_SIGNAL, {
                "st": "lobbying",
                "sd": r[1],
                "tk": None,
                "act": entity,
                "dir": None,
                "sub": "lobbying",
                "mag": r[2],
                "desc": f"Lobbying activity: {entity}",
                "data": _jsonify(r[3]),
                "conf": "confirmed",
                "src": "quiver_lobbying",
            })
            count += 1
        return count


def populate_from_insider(engine) -> int:
    """Convert qq:insider raw_series entries into signal_data."""
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT series_id, obs_date, value, raw_payload
            FROM raw_series
            WHERE series_id LIKE 'qq:insider:%'
            ORDER BY obs_date
        """)).fetchall()

        count = 0
        for r in rows:
            parts = r[0].split(":")
            ticker = parts[2] if len(parts) > 2 else None
            payload = r[3] if isinstance(r[3], dict) else (json.loads(r[3]) if r[3] else {})
            insider = payload.get("name", "insider") if payload else "insider"

            conn.execute(_INSERT_SIGNAL, {
                "st": "insider_trade",
                "sd": r[1],
                "tk": ticker,
                "act": insider,
                "dir": "BULL" if r[2] > 0 else "BEAR",
                "sub": "insider_trade",
                "mag": abs(r[2]),
                "desc": f"Insider {('buy' if r[2] > 0 else 'sell')} {ticker} by {insider}",
                "data": _jsonify(r[3]),
                "conf": "confirmed",
                "src": "quiver_insider",
            })
            count += 1
        return count


def populate_from_news(engine) -> int:
    """Convert NEWS raw_series entries into signal_data."""
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT series_id, obs_date, value, raw_payload
            FROM raw_series
            WHERE series_id LIKE 'NEWS:%'
            ORDER BY obs_date
        """)).fetchall()

        count = 0
        for r in rows:
            parts = r[0].split(":")
            ticker = parts[1] if len(parts) > 1 else None
            payload = r[3] if isinstance(r[3], dict) else (json.loads(r[3]) if r[3] else {})

            conn.execute(_INSERT_SIGNAL, {
                "st": "news_event",
                "sd": r[1],
                "tk": ticker,
                "act": payload.get("source", "news"),
                "dir": "BULL" if r[2] > 0 else "BEAR" if r[2] < 0 else "NEUTRAL",
                "sub": "news_event",
                "mag": abs(r[2]),
                "desc": payload.get("title", f"News event for {ticker}"),
                "data": _jsonify(r[3]),
                "conf": "derived",
                "src": "news_aggregator",
            })
            count += 1
        return count


def main():
    engine = get_engine()

    # Check if already populated
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM signal_data")).scalar()
        if count > 0:
            log.info("signal_data already has {n} rows — skipping population", n=count)
            return

    log.info("=== Populating signal_data from existing DB sources ===")

    sources = [
        ("whale_flow", populate_from_whale_flows),
        ("gov_contract", populate_from_gov_contracts),
        ("legislation", populate_from_legislation),
        ("options_flow", populate_from_options_flow),
        ("social", populate_from_social),
        ("congressional", populate_from_congressional_raw),
        ("lobbying", populate_from_lobbying),
        ("insider", populate_from_insider),
        ("news", populate_from_news),
    ]

    total = 0
    for name, func in sources:
        try:
            n = func(engine)
            log.info("  {name}: {n} signals", name=name, n=n)
            total += n
        except Exception as exc:
            log.error("  {name} failed: {e}", name=name, e=str(exc))

    log.info("=== Done: {n} total signals inserted into signal_data ===", n=total)

    # Verify
    with engine.connect() as conn:
        types = conn.execute(text(
            "SELECT signal_type, COUNT(*) as cnt FROM signal_data GROUP BY signal_type ORDER BY cnt DESC"
        )).fetchall()
        for t in types:
            log.info("  {type}: {cnt}", type=t[0], cnt=t[1])


if __name__ == "__main__":
    main()
