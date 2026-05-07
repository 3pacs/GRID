#!/usr/bin/env python3
"""
News-to-Signals — reads parsed news from 5 intelligence tables and emits
unified signal_data rows with proper actor matching.

Input tables (checked with IF EXISTS):
  1. business_events     — category, tickers[], headline, direction, estimated_bps, confidence, published_at
  2. deal_pipeline       — deal_type, stage, tickers[], acquirer, target, direction, probability, detected_at
  3. news_momentum       — ticker, signal_type, direction, magnitude, confidence, computed_at
  4. earnings_analysis   — ticker, filing_date, tone_label, tone_shift, confidence, computed_at
  5. sec_material_facts  — ticker, item_number, item_name, direction, estimated_bps, confidence, filing_date

Output: signal_data with dedup via ON CONFLICT DO NOTHING.

Usage:
    python scripts/news_to_signals.py          # one-shot
    python scripts/news_to_signals.py --dry-run # count only, no writes
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

# Ensure grid root is on sys.path
_GRID_DIR = str(Path(__file__).resolve().parent.parent)
os.chdir(_GRID_DIR)
if _GRID_DIR not in sys.path:
    sys.path.insert(0, _GRID_DIR)

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ─── Confidence floor ────────────────────────────────────────────────
CONFIDENCE_FLOOR = 0.3


# ─── Company Aliases (critical for false-positive reduction) ─────────
_COMPANY_ALIASES: dict[str, str] = {
    # Official names -> ticker
    "apple": "AAPL", "apple inc": "AAPL", "apple computer": "AAPL",
    "amazon": "AMZN", "amazon.com": "AMZN", "aws": "AMZN",
    "google": "GOOGL", "alphabet": "GOOGL",
    "meta": "META", "meta platforms": "META", "facebook": "META",
    "microsoft": "MSFT",
    "tesla": "TSLA",
    "nvidia": "NVDA",
    "goldman sachs": "GS", "goldman": "GS",
    "jp morgan": "JPM", "jpmorgan": "JPM", "morgan chase": "JPM",
    "morgan stanley": "MS",
    "bank of america": "BAC", "bofa": "BAC",
    "wells fargo": "WFC",
    "berkshire hathaway": "BRK-B", "berkshire": "BRK-B",
    "johnson & johnson": "JNJ", "j&j": "JNJ",
    "procter & gamble": "PG", "p&g": "PG",
    "coca-cola": "KO", "coke": "KO",
    "walmart": "WMT",
    "unitedhealth": "UNH",
    "broadcom": "AVGO",
    "salesforce": "CRM",
    "netflix": "NFLX",
    "adobe": "ADBE",
    "intel": "INTC",
    "amd": "AMD", "advanced micro devices": "AMD",
    "eli lilly": "LLY", "lilly": "LLY",
    "pfizer": "PFE",
    "disney": "DIS", "walt disney": "DIS",
}


# ─── Entity Resolver ─────────────────────────────────────────────────

class EntityResolver:
    """Maps company names/tickers to actor IDs in the actors table."""

    def __init__(self, engine: Engine) -> None:
        self._ticker_to_actor_id: dict[str, str] = {}
        self._name_to_ticker: dict[str, str] = {}
        self._alias_map: dict[str, str] = dict(_COMPANY_ALIASES)

        self._load_company_profiles(engine)
        self._load_actors(engine)

    # ── loaders ──

    def _load_company_profiles(self, engine: Engine) -> None:
        """Load company_profiles: ticker -> name mapping."""
        try:
            with engine.connect() as conn:
                rows = conn.execute(text(
                    "SELECT ticker, name FROM company_profiles WHERE name IS NOT NULL"
                )).fetchall()
            for ticker, name in rows:
                if ticker and name:
                    self._name_to_ticker[name.lower().strip()] = ticker.upper()
            log.info("EntityResolver: loaded {n} company profiles", n=len(rows))
        except Exception as exc:
            log.debug("company_profiles not available: {e}", e=str(exc))

    def _load_actors(self, engine: Engine) -> None:
        """Load actors: name -> id mapping for corporation actors."""
        try:
            with engine.connect() as conn:
                rows = conn.execute(text(
                    "SELECT id, name FROM actors "
                    "WHERE category NOT IN ('offshore', 'icij') "
                    "AND name IS NOT NULL"
                )).fetchall()
            for actor_id, name in rows:
                if actor_id and name:
                    # Build ticker -> actor_id for actors like corp_AAPL
                    if actor_id.startswith("corp_"):
                        ticker = actor_id[5:].upper()
                        self._ticker_to_actor_id[ticker] = actor_id
                    # Also allow name-based lookup
                    self._name_to_ticker[name.lower().strip()] = actor_id
            log.info("EntityResolver: loaded {n} actors", n=len(rows))
        except Exception as exc:
            log.debug("actors table not available: {e}", e=str(exc))

    # ── resolution ──

    def resolve_ticker(self, ticker: str | None) -> str | None:
        """Given an uppercase ticker, return actor_id or None."""
        if not ticker:
            return None
        t = ticker.strip().upper()
        if len(t) < 2:
            return None
        return self._ticker_to_actor_id.get(t)

    def resolve_name(self, name: str | None) -> tuple[str | None, str | None]:
        """Given a company name, return (ticker, actor_id) or (None, None).

        False-positive prevention:
          - Only match names >= 4 characters
          - Require word-boundary matches for short names
        """
        if not name:
            return None, None
        cleaned = name.lower().strip()
        if len(cleaned) < 4:
            return None, None

        # 1. Direct alias match
        if cleaned in self._alias_map:
            ticker = self._alias_map[cleaned]
            return ticker, self._ticker_to_actor_id.get(ticker)

        # 2. Name -> ticker from company_profiles
        if cleaned in self._name_to_ticker:
            val = self._name_to_ticker[cleaned]
            # val might be a ticker (from company_profiles) or actor_id (from actors)
            if val.startswith("corp_") or "_" in val:
                return None, val
            return val, self._ticker_to_actor_id.get(val)

        # 3. Word-boundary search in aliases (for multi-word aliases)
        for alias, ticker in self._alias_map.items():
            if len(alias) >= 4 and re.search(r"\b" + re.escape(alias) + r"\b", cleaned):
                return ticker, self._ticker_to_actor_id.get(ticker)

        return None, None

    def resolve_ticker_or_name(
        self, ticker: str | None, name: str | None
    ) -> tuple[str | None, str | None]:
        """Try ticker first, then fall back to name resolution.

        Returns (ticker, actor_id).
        """
        if ticker:
            t = ticker.strip().upper()
            if len(t) >= 2 and t.isupper():
                actor_id = self.resolve_ticker(t)
                return t, actor_id

        t, actor_id = self.resolve_name(name)
        return t, actor_id


# ─── Helpers ──────────────────────────────────────────────────────────

def _table_exists(conn: Any, table_name: str) -> bool:
    """Check if a table exists in the database."""
    row = conn.execute(text(
        "SELECT EXISTS ("
        "  SELECT 1 FROM information_schema.tables "
        "  WHERE table_name = :tbl"
        ")"
    ), {"tbl": table_name}).scalar()
    return bool(row)


def _desc_hash(desc: str) -> str:
    """Short stable hash of a description for dedup."""
    return hashlib.md5(desc.encode("utf-8", errors="replace")).hexdigest()[:12]


def _clamp(val: float, lo: float = 0.0, hi: float = 10.0) -> float:
    return max(lo, min(hi, val))


def _to_date(val: Any) -> date | None:
    """Coerce various timestamp/date types to a plain date."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    try:
        return datetime.fromisoformat(str(val)).date()
    except (ValueError, TypeError):
        return None


def _insert_signal(
    conn: Any,
    *,
    signal_type: str,
    signal_date: date,
    ticker: str | None,
    actor: str | None,
    direction: str,
    magnitude: float,
    description: str,
    data: dict[str, Any],
    confidence: str,
    source_id: str,
) -> bool:
    """Insert a single signal_data row with dedup. Returns True if inserted."""
    conn.execute(text("""
        INSERT INTO signal_data
            (signal_type, signal_date, ticker, actor, direction,
             magnitude, description, data, confidence, source_id, created_at)
        VALUES (:stype, :sdate, :ticker, :actor, :dir,
                :mag, :desc, :data, :conf, :src, NOW())
        ON CONFLICT DO NOTHING
    """), {
        "stype": signal_type,
        "sdate": signal_date,
        "ticker": ticker,
        "actor": actor,
        "dir": direction,
        "mag": magnitude,
        "desc": description,
        "data": json.dumps(data),
        "conf": confidence,
        "src": source_id,
    })
    return True


# ─── Emitters ─────────────────────────────────────────────────────────

def _emit_business_events(engine: Engine, resolver: EntityResolver) -> int:
    """business_events -> signal_data."""
    count = 0
    with engine.begin() as conn:
        if not _table_exists(conn, "business_events"):
            log.debug("business_events table does not exist, skipping")
            return 0

        rows = conn.execute(text("""
            SELECT be.id, be.category, be.tickers, be.headline, be.direction,
                   be.estimated_bps, be.confidence, be.published_at
            FROM business_events be
            WHERE be.confidence >= :floor
              AND NOT EXISTS (
                  SELECT 1 FROM signal_data sd
                  WHERE sd.source_id = CONCAT('biz_event:', be.id::text)
              )
            ORDER BY be.published_at DESC NULLS LAST
            LIMIT 5000
        """), {"floor": CONFIDENCE_FLOOR}).fetchall()

        for row in rows:
            be_id, category, tickers, headline, direction, est_bps, conf, published_at = row
            sig_date = _to_date(published_at)
            if sig_date is None:
                continue

            # First ticker from array
            ticker = tickers[0] if tickers and len(tickers) > 0 else None
            if ticker:
                ticker = ticker.strip().upper()

            # Actor from headline resolution
            _, actor_id = resolver.resolve_ticker_or_name(ticker, headline)

            # Normalize magnitude: estimated_bps / 100, clamped 0-10
            magnitude = _clamp((est_bps or 0) / 100.0)

            sig_type = f"news_{(category or 'general').lower().replace(' ', '_')}"
            source_id = f"biz_event:{be_id}"

            _insert_signal(
                conn,
                signal_type=sig_type,
                signal_date=sig_date,
                ticker=ticker,
                actor=actor_id,
                direction=direction or "neutral",
                magnitude=magnitude,
                description=headline or "",
                data={
                    "category": category,
                    "estimated_bps": est_bps,
                    "all_tickers": tickers or [],
                },
                confidence="derived" if (conf or 0) < 0.7 else "confirmed",
                source_id=source_id,
            )
            count += 1

    log.info("business_events -> signal_data: {n} emitted", n=count)
    return count


def _emit_deal_signals(engine: Engine, resolver: EntityResolver) -> int:
    """deal_pipeline -> signal_data."""
    count = 0
    with engine.begin() as conn:
        if not _table_exists(conn, "deal_pipeline"):
            log.debug("deal_pipeline table does not exist, skipping")
            return 0

        rows = conn.execute(text("""
            SELECT dp.id, dp.deal_type, dp.stage, dp.tickers, dp.acquirer,
                   dp.target, dp.headline, dp.direction, dp.probability,
                   dp.confidence, dp.detected_at, dp.deal_value_usd
            FROM deal_pipeline dp
            WHERE dp.confidence >= :floor
              AND NOT EXISTS (
                  SELECT 1 FROM signal_data sd
                  WHERE sd.source_id = CONCAT('deal:', dp.id::text)
              )
            ORDER BY dp.detected_at DESC NULLS LAST
            LIMIT 5000
        """), {"floor": CONFIDENCE_FLOOR}).fetchall()

        for row in rows:
            (dp_id, deal_type, stage, tickers, acquirer, target,
             headline, direction, probability, conf, detected_at, deal_value) = row

            sig_date = _to_date(detected_at)
            if sig_date is None:
                continue

            ticker = tickers[0] if tickers and len(tickers) > 0 else None
            if ticker:
                ticker = ticker.strip().upper()

            # Resolve acquirer and target to actor IDs
            _, acq_actor = resolver.resolve_ticker_or_name(None, acquirer)
            _, tgt_actor = resolver.resolve_ticker_or_name(ticker, target)

            # Use target actor primarily, fall back to acquirer
            actor = tgt_actor or acq_actor

            sig_type = f"deal_{(deal_type or 'unknown').lower().replace(' ', '_')}"
            magnitude = _clamp((probability or 0.25) * 10.0)
            source_id = f"deal:{dp_id}"

            _insert_signal(
                conn,
                signal_type=sig_type,
                signal_date=sig_date,
                ticker=ticker,
                actor=actor,
                direction=direction or "neutral",
                magnitude=magnitude,
                description=headline or f"{acquirer or '?'} -> {target or '?'}",
                data={
                    "deal_type": deal_type,
                    "stage": stage,
                    "acquirer": acquirer,
                    "target": target,
                    "probability": probability,
                    "deal_value_usd": deal_value,
                    "all_tickers": tickers or [],
                },
                confidence="derived" if (conf or 0) < 0.7 else "confirmed",
                source_id=source_id,
            )
            count += 1

    log.info("deal_pipeline -> signal_data: {n} emitted", n=count)
    return count


def _emit_momentum_signals(engine: Engine, resolver: EntityResolver) -> int:
    """news_momentum -> signal_data."""
    count = 0
    with engine.begin() as conn:
        if not _table_exists(conn, "news_momentum"):
            log.debug("news_momentum table does not exist, skipping")
            return 0

        rows = conn.execute(text("""
            SELECT nm.id, nm.ticker, nm.signal_type, nm.direction,
                   nm.magnitude, nm.confidence, nm.computed_at,
                   nm.sentiment_velocity, nm.article_count
            FROM news_momentum nm
            WHERE nm.confidence >= :floor
              AND NOT EXISTS (
                  SELECT 1 FROM signal_data sd
                  WHERE sd.source_id = CONCAT('momentum:', nm.id::text)
              )
            ORDER BY nm.computed_at DESC NULLS LAST
            LIMIT 5000
        """), {"floor": CONFIDENCE_FLOOR}).fetchall()

        for row in rows:
            (nm_id, ticker, sig_type_raw, direction,
             magnitude, conf, computed_at,
             sentiment_vel, article_count) = row

            sig_date = _to_date(computed_at)
            if sig_date is None:
                continue

            if ticker:
                ticker = ticker.strip().upper()

            actor_id = resolver.resolve_ticker(ticker)

            sig_type = f"momentum_{(sig_type_raw or 'general').lower().replace(' ', '_')}"
            mag = _clamp(magnitude or 0.0)
            source_id = f"momentum:{nm_id}"

            _insert_signal(
                conn,
                signal_type=sig_type,
                signal_date=sig_date,
                ticker=ticker,
                actor=actor_id,
                direction=direction or "neutral",
                magnitude=mag,
                description=f"{ticker} momentum {sig_type_raw}: {direction}",
                data={
                    "original_signal_type": sig_type_raw,
                    "sentiment_velocity": sentiment_vel,
                    "article_count": article_count,
                },
                confidence="derived" if (conf or 0) < 0.7 else "confirmed",
                source_id=source_id,
            )
            count += 1

    log.info("news_momentum -> signal_data: {n} emitted", n=count)
    return count


def _emit_earnings_signals(engine: Engine, resolver: EntityResolver) -> int:
    """earnings_analysis -> signal_data."""
    count = 0
    with engine.begin() as conn:
        if not _table_exists(conn, "earnings_analysis"):
            log.debug("earnings_analysis table does not exist, skipping")
            return 0

        rows = conn.execute(text("""
            SELECT ea.id, ea.ticker, ea.filing_date, ea.tone_label,
                   ea.tone_shift, ea.confidence, ea.overall_tone,
                   ea.positive_count, ea.negative_count
            FROM earnings_analysis ea
            WHERE ea.confidence >= :floor
              AND NOT EXISTS (
                  SELECT 1 FROM signal_data sd
                  WHERE sd.source_id = CONCAT('earnings:', ea.id::text)
              )
            ORDER BY ea.filing_date DESC NULLS LAST
            LIMIT 5000
        """), {"floor": CONFIDENCE_FLOOR}).fetchall()

        for row in rows:
            (ea_id, ticker, filing_date, tone_label,
             tone_shift, conf, overall_tone,
             pos_count, neg_count) = row

            sig_date = _to_date(filing_date)
            if sig_date is None:
                continue

            if ticker:
                ticker = ticker.strip().upper()

            actor_id = resolver.resolve_ticker(ticker)

            # Map tone_label to direction
            tone_lower = (tone_label or "neutral").lower()
            if tone_lower in ("positive", "bullish", "optimistic"):
                direction = "bullish"
            elif tone_lower in ("negative", "bearish", "pessimistic"):
                direction = "bearish"
            else:
                direction = "neutral"

            magnitude = _clamp(abs(tone_shift or 0.0) * 10.0)
            source_id = f"earnings:{ea_id}"

            _insert_signal(
                conn,
                signal_type="earnings_tone",
                signal_date=sig_date,
                ticker=ticker,
                actor=actor_id,
                direction=direction,
                magnitude=magnitude,
                description=f"{ticker} earnings tone: {tone_label} (shift={tone_shift:.2f})"
                            if tone_shift is not None
                            else f"{ticker} earnings tone: {tone_label}",
                data={
                    "tone_label": tone_label,
                    "tone_shift": tone_shift,
                    "overall_tone": overall_tone,
                    "positive_count": pos_count,
                    "negative_count": neg_count,
                },
                confidence="derived" if (conf or 0) < 0.7 else "confirmed",
                source_id=source_id,
            )
            count += 1

    log.info("earnings_analysis -> signal_data: {n} emitted", n=count)
    return count


def _emit_sec_signals(engine: Engine, resolver: EntityResolver) -> int:
    """sec_material_facts -> signal_data."""
    count = 0
    with engine.begin() as conn:
        if not _table_exists(conn, "sec_material_facts"):
            log.debug("sec_material_facts table does not exist, skipping")
            return 0

        rows = conn.execute(text("""
            SELECT sm.id, sm.ticker, sm.filing_date, sm.item_number,
                   sm.item_name, sm.description, sm.direction,
                   sm.estimated_bps, sm.confidence
            FROM sec_material_facts sm
            WHERE sm.confidence >= :floor
              AND NOT EXISTS (
                  SELECT 1 FROM signal_data sd
                  WHERE sd.source_id = CONCAT('sec_fact:', sm.id::text)
              )
            ORDER BY sm.filing_date DESC NULLS LAST
            LIMIT 5000
        """), {"floor": CONFIDENCE_FLOOR}).fetchall()

        for row in rows:
            (sm_id, ticker, filing_date, item_number,
             item_name, description, direction,
             est_bps, conf) = row

            sig_date = _to_date(filing_date)
            if sig_date is None:
                continue

            if ticker:
                ticker = ticker.strip().upper()

            actor_id = resolver.resolve_ticker(ticker)

            # sec_8.01 -> sec_8_01
            item_num_clean = (item_number or "unknown").replace(".", "_")
            sig_type = f"sec_{item_num_clean}"
            magnitude = _clamp((est_bps or 0) / 100.0)
            source_id = f"sec_fact:{sm_id}"

            _insert_signal(
                conn,
                signal_type=sig_type,
                signal_date=sig_date,
                ticker=ticker,
                actor=actor_id,
                direction=direction or "neutral",
                magnitude=magnitude,
                description=description or f"{ticker} SEC {item_number}: {item_name}",
                data={
                    "item_number": item_number,
                    "item_name": item_name,
                    "estimated_bps": est_bps,
                },
                confidence="derived" if (conf or 0) < 0.7 else "confirmed",
                source_id=source_id,
            )
            count += 1

    log.info("sec_material_facts -> signal_data: {n} emitted", n=count)
    return count


# ─── Main ─────────────────────────────────────────────────────────────

def main(dry_run: bool = False) -> int:
    """Run all news-to-signal emitters. Returns total signals emitted."""
    from db import get_engine

    engine = get_engine()
    resolver = EntityResolver(engine)

    if dry_run:
        log.info("DRY RUN — counting available rows only")
        total = 0
        with engine.connect() as conn:
            for tbl in ("business_events", "deal_pipeline", "news_momentum",
                        "earnings_analysis", "sec_material_facts"):
                if _table_exists(conn, tbl):
                    n = conn.execute(text(f"SELECT COUNT(*) FROM {tbl}")).scalar()  # noqa: S608
                    log.info("  {tbl}: {n} rows", tbl=tbl, n=n)
                    total += n
                else:
                    log.info("  {tbl}: does not exist", tbl=tbl)
        log.info("DRY RUN total source rows: {n}", n=total)
        return 0

    total = 0
    total += _emit_business_events(engine, resolver)
    total += _emit_deal_signals(engine, resolver)
    total += _emit_momentum_signals(engine, resolver)
    total += _emit_earnings_signals(engine, resolver)
    total += _emit_sec_signals(engine, resolver)

    log.info("News-to-signals complete: {n} signals emitted", n=total)
    return total


if __name__ == "__main__":
    import argparse

    log.remove()
    log.add(sys.stderr, level="INFO")

    parser = argparse.ArgumentParser(description="News intelligence -> signal_data")
    parser.add_argument("--dry-run", action="store_true", help="Count only, no writes")
    args = parser.parse_args()

    total = main(dry_run=args.dry_run)
    sys.exit(0 if total >= 0 else 1)
