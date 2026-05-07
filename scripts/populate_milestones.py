#!/usr/bin/env python3
"""
Populate company_milestones from ALL intelligence sources.

Reads from:
  1. business_events     — parsed news events with category/direction/impact
  2. deal_pipeline       — M&A pipeline deals
  3. sec_material_facts  — 8-K filings with item-level classification
  4. earnings_analysis   — earnings call tone data
  5. signal_data         — insider trades, congressional trades (high magnitude)
  6. oracle_predictions  — model predictions (high confidence)
  7. catalyst_calendar   — trial readouts, FDA decisions

Each source maps into the existing company_milestones table schema:
  (ticker, milestone_type, announced_date, description, probability,
   confidence_source, value_impact_pct, status, source_url, notes)

Dedup: ON CONFLICT (ticker, milestone_type, announced_date, description[:200])
is handled via a composite check before INSERT.

Usage:
    python scripts/populate_milestones.py               # full run
    python scripts/populate_milestones.py --dry-run      # count only
    python scripts/populate_milestones.py --source=deals # single source
"""

from __future__ import annotations

import os
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


# ── Category → milestone_type mapping for business_events ─────────────
_BIZ_CATEGORY_MAP: dict[str, str] = {
    "executive_change": "STRATEGIC",
    "product_launch": "PRODUCT_LAUNCH",
    "restructuring": "STRATEGIC",
    "regulatory_action": "REGULATORY",
    "legal_action": "REGULATORY",
    "capital_raise": "M_AND_A",
    "credit_event": "STRATEGIC",
    "guidance_change": "EARNINGS_GUIDANCE",
    "earnings_surprise": "EARNINGS_GUIDANCE",
    "contract_win": "EXPANSION",
    "supply_chain": "STRATEGIC",
    "competitive": "PRODUCT_LAUNCH",
    "m_and_a": "M_AND_A",
    "merger": "M_AND_A",
    "acquisition": "M_AND_A",
    "divestiture": "M_AND_A",
    "partnership": "EXPANSION",
    "ipo": "M_AND_A",
    "spinoff": "M_AND_A",
    "bankruptcy": "STRATEGIC",
}

# ── SEC 8-K item_number → milestone_type ──────────────────────────────
_SEC_ITEM_MAP: dict[str, str] = {
    "1.01": "EXPANSION",       # Entry into a material agreement
    "1.02": "STRATEGIC",       # Termination of a material agreement
    "1.03": "STRATEGIC",       # Bankruptcy or receivership
    "2.01": "M_AND_A",         # Completion of acquisition/disposition
    "2.02": "EARNINGS_GUIDANCE",  # Results of operations
    "2.03": "DEBT_TARGET",     # Creation of direct financial obligation
    "2.04": "STRATEGIC",       # Triggering events (default/acceleration)
    "2.05": "STRATEGIC",       # Costs associated with exit/disposal
    "2.06": "STRATEGIC",       # Material impairments
    "3.01": "REGULATORY",      # Notice of delisting or non-compliance
    "3.02": "STRATEGIC",       # Unregistered sales of equity
    "3.03": "STRATEGIC",       # Material modification to shareholder rights
    "4.01": "REGULATORY",      # Changes in registrant's certifying accountant
    "4.02": "REGULATORY",      # Non-reliance on prior financial statements
    "5.01": "STRATEGIC",       # Change in control
    "5.02": "STRATEGIC",       # Departure/election of directors/officers
    "5.03": "REGULATORY",      # Amendments to articles/bylaws
    "5.05": "DIVIDEND",        # Amendments to code of ethics
    "5.06": "BUYBACK",         # Change in shell company status
    "5.07": "STRATEGIC",       # Submission of matters to shareholder vote
    "7.01": "REGULATORY",      # Regulation FD disclosure
    "8.01": "STRATEGIC",       # Other events
    "9.01": "REGULATORY",      # Financial statements and exhibits
}

# ── Direction to probability mapping ──────────────────────────────────
_DIR_PROB: dict[str, float] = {
    "bullish": 0.65,
    "bearish": 0.35,
    "neutral": 0.50,
}


def _table_exists(conn: Any, table_name: str) -> bool:
    """Check if a table exists in the database."""
    row = conn.execute(text(
        "SELECT EXISTS ("
        "  SELECT 1 FROM information_schema.tables "
        "  WHERE table_name = :tbl"
        ")"
    ), {"tbl": table_name}).scalar()
    return bool(row)


def _direction_to_status(direction: str | None) -> str:
    """Map event direction to milestone status."""
    if direction == "bullish":
        return "ON_TRACK"
    if direction == "bearish":
        return "BEHIND"
    return "PENDING"


def _clamp_probability(val: float | None) -> float:
    """Clamp probability to [0, 1]."""
    if val is None:
        return 0.5
    return max(0.0, min(1.0, float(val)))


def _bps_to_pct(bps: int | None) -> float | None:
    """Convert basis points to percentage (100 bps = 1%)."""
    if bps is None or bps == 0:
        return None
    return float(bps) / 100.0


def _safe_desc(desc: str | None, fallback: str = "Event") -> str:
    """Ensure description is non-empty and truncated to 500 chars."""
    val = (desc or fallback).strip()
    if not val:
        val = fallback
    return val[:500]


def _ensure_table(engine: Engine) -> None:
    """Ensure company_milestones table exists with required indexes."""
    with engine.begin() as conn:
        if _table_exists(conn, "company_milestones"):
            log.info("company_milestones table already exists")
            return

        log.info("Creating company_milestones table")
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS company_milestones (
                id                  BIGSERIAL PRIMARY KEY,
                ticker              TEXT NOT NULL,
                milestone_type      TEXT NOT NULL,
                announced_date      DATE NOT NULL,
                target_date         DATE,
                actual_date         DATE,
                description         TEXT NOT NULL,
                target_value        DOUBLE PRECISION,
                target_unit         TEXT,
                actual_value        DOUBLE PRECISION,
                achievement_pct     DOUBLE PRECISION,
                probability         DOUBLE PRECISION NOT NULL DEFAULT 0.5,
                confidence_source   TEXT DEFAULT 'CALCULATED',
                value_impact_ps     DOUBLE PRECISION,
                value_impact_pct    DOUBLE PRECISION,
                status              TEXT NOT NULL DEFAULT 'PENDING',
                source_url          TEXT,
                notes               TEXT,
                created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_milestones_ticker "
            "ON company_milestones (ticker)"
        ))
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_milestones_ticker_status "
            "ON company_milestones (ticker, status)"
        ))
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_milestones_target_date "
            "ON company_milestones (target_date)"
        ))
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_milestones_type "
            "ON company_milestones (milestone_type)"
        ))
        log.info("company_milestones table created with indexes")


def _milestone_exists(conn: Any, ticker: str, mtype: str,
                      ann_date: date, desc: str) -> bool:
    """Check if a milestone already exists (dedup)."""
    row = conn.execute(text("""
        SELECT EXISTS (
            SELECT 1 FROM company_milestones
            WHERE ticker = :ticker
              AND milestone_type = :mtype
              AND announced_date = :ann_date
              AND LEFT(description, 200) = LEFT(:desc, 200)
        )
    """), {
        "ticker": ticker,
        "mtype": mtype,
        "ann_date": ann_date,
        "desc": desc,
    }).scalar()
    return bool(row)


def _insert_milestone(
    conn: Any,
    ticker: str,
    milestone_type: str,
    announced_date: date,
    description: str,
    probability: float = 0.5,
    confidence_source: str = "CALCULATED",
    value_impact_pct: float | None = None,
    status: str = "PENDING",
    source_url: str | None = None,
    notes: str | None = None,
    target_date: date | None = None,
) -> bool:
    """Insert a single milestone. Returns True if inserted, False if skipped."""
    ticker = ticker.upper().strip()
    if not ticker or len(ticker) > 10:
        return False

    description = _safe_desc(description)
    probability = _clamp_probability(probability)

    if _milestone_exists(conn, ticker, milestone_type, announced_date, description):
        return False

    conn.execute(text("""
        INSERT INTO company_milestones (
            ticker, milestone_type, announced_date, target_date,
            description, probability, confidence_source,
            value_impact_pct, status, source_url, notes
        ) VALUES (
            :ticker, :mtype, :ann_date, :tgt_date,
            :desc, :prob, :conf_src,
            :vi_pct, :status, :src_url, :notes
        )
    """), {
        "ticker": ticker,
        "mtype": milestone_type,
        "ann_date": announced_date,
        "tgt_date": target_date,
        "desc": description,
        "prob": probability,
        "conf_src": confidence_source,
        "vi_pct": value_impact_pct,
        "status": status,
        "src_url": source_url,
        "notes": notes,
    })
    return True


# ── Source 1: business_events ─────────────────────────────────────────

def _from_business_events(engine: Engine) -> int:
    """Populate milestones from business_events table."""
    count = 0
    with engine.begin() as conn:
        if not _table_exists(conn, "business_events"):
            log.debug("business_events table does not exist, skipping")
            return 0

        rows = conn.execute(text("""
            SELECT category, tickers, headline, description,
                   direction, estimated_bps, confidence,
                   published_at, article_url, source
            FROM business_events
            WHERE confidence >= 0.3
            ORDER BY published_at DESC NULLS LAST
            LIMIT 50000
        """)).fetchall()

        for r in rows:
            category = (r[0] or "").lower()
            tickers = r[1] or []
            headline = r[2] or ""
            desc = r[3] or headline
            direction = (r[4] or "neutral").lower()
            estimated_bps = r[5]
            confidence = r[6] or 0.5
            published_at = r[7]
            article_url = r[8]
            source = r[9] or ""

            milestone_type = _BIZ_CATEGORY_MAP.get(category, "STRATEGIC")
            ann_date = _extract_date(published_at) or date.today()
            prob = _clamp_probability(confidence)
            status = _direction_to_status(direction)
            vi_pct = _bps_to_pct(estimated_bps)

            for ticker in tickers:
                if _insert_milestone(
                    conn,
                    ticker=ticker,
                    milestone_type=milestone_type,
                    announced_date=ann_date,
                    description=_safe_desc(headline, f"{category} event"),
                    probability=prob,
                    confidence_source="CALCULATED",
                    value_impact_pct=vi_pct,
                    status=status,
                    source_url=article_url or None,
                    notes=f"Source: {source}. {desc[:200]}" if desc else f"Source: {source}",
                ):
                    count += 1

    log.info("business_events -> company_milestones: {n} inserted", n=count)
    return count


# ── Source 2: deal_pipeline ───────────────────────────────────────────

def _from_deal_pipeline(engine: Engine) -> int:
    """Populate milestones from deal_pipeline table."""
    count = 0
    with engine.begin() as conn:
        if not _table_exists(conn, "deal_pipeline"):
            log.debug("deal_pipeline table does not exist, skipping")
            return 0

        rows = conn.execute(text("""
            SELECT deal_type, stage, tickers, acquirer, target,
                   headline, direction, probability, confidence,
                   detected_at, deal_value_usd, article_url
            FROM deal_pipeline
            WHERE confidence >= 0.3
            ORDER BY detected_at DESC NULLS LAST
            LIMIT 50000
        """)).fetchall()

        for r in rows:
            deal_type = r[0] or "M&A"
            stage = r[1] or "REPORTED"
            tickers = r[2] or []
            acquirer = r[3] or ""
            target_co = r[4] or ""
            headline = r[5] or f"{deal_type}: {acquirer} / {target_co}"
            (r[6] or "neutral").lower()
            probability = r[7] or 0.25
            r[8] or 0.5
            detected_at = r[9]
            deal_value = r[10]
            article_url = r[11]

            ann_date = _extract_date(detected_at) or date.today()
            status = _stage_to_status(stage)
            vi_pct = _deal_value_impact(deal_value)

            notes_parts = [f"Deal type: {deal_type}", f"Stage: {stage}"]
            if acquirer:
                notes_parts.append(f"Acquirer: {acquirer}")
            if target_co:
                notes_parts.append(f"Target: {target_co}")
            if deal_value:
                notes_parts.append(f"Value: ${deal_value:,.0f}")

            for ticker in tickers:
                if _insert_milestone(
                    conn,
                    ticker=ticker,
                    milestone_type="M_AND_A",
                    announced_date=ann_date,
                    description=_safe_desc(headline, f"{deal_type} deal"),
                    probability=_clamp_probability(probability),
                    confidence_source="MARKET",
                    value_impact_pct=vi_pct,
                    status=status,
                    source_url=article_url or None,
                    notes="; ".join(notes_parts),
                ):
                    count += 1

    log.info("deal_pipeline -> company_milestones: {n} inserted", n=count)
    return count


def _stage_to_status(stage: str) -> str:
    """Map deal stage to milestone status."""
    stage_upper = stage.upper()
    if stage_upper in ("COMPLETED", "CLOSED"):
        return "ACHIEVED"
    if stage_upper in ("TERMINATED", "WITHDRAWN", "FAILED"):
        return "MISSED"
    if stage_upper in ("APPROVED", "DEFINITIVE"):
        return "ON_TRACK"
    return "PENDING"


def _deal_value_impact(deal_value_usd: float | None) -> float | None:
    """Estimate deal impact as percentage. Rough heuristic."""
    if deal_value_usd is None:
        return None
    if deal_value_usd > 50_000_000_000:
        return 15.0
    if deal_value_usd > 10_000_000_000:
        return 10.0
    if deal_value_usd > 1_000_000_000:
        return 5.0
    if deal_value_usd > 100_000_000:
        return 2.0
    return 1.0


# ── Source 3: sec_material_facts ──────────────────────────────────────

def _from_sec_facts(engine: Engine) -> int:
    """Populate milestones from sec_material_facts (8-K filings)."""
    count = 0
    with engine.begin() as conn:
        if not _table_exists(conn, "sec_material_facts"):
            log.debug("sec_material_facts table does not exist, skipping")
            return 0

        rows = conn.execute(text("""
            SELECT ticker, filing_date, item_number, item_name,
                   description, direction, estimated_bps, confidence
            FROM sec_material_facts
            WHERE confidence >= 0.3
            ORDER BY filing_date DESC NULLS LAST
            LIMIT 50000
        """)).fetchall()

        for r in rows:
            ticker = r[0] or ""
            filing_date = r[1]
            item_number = (r[2] or "8.01").strip()
            item_name = r[3] or ""
            desc = r[4] or item_name
            direction = (r[5] or "neutral").lower()
            estimated_bps = r[6]
            confidence = r[7] or 0.5

            milestone_type = _SEC_ITEM_MAP.get(item_number, "STRATEGIC")
            ann_date = filing_date or date.today()
            status = _direction_to_status(direction)
            vi_pct = _bps_to_pct(estimated_bps)

            title = f"8-K Item {item_number}: {item_name}" if item_name else f"8-K Item {item_number}"

            if _insert_milestone(
                conn,
                ticker=ticker,
                milestone_type=milestone_type,
                announced_date=ann_date,
                description=_safe_desc(title),
                probability=_clamp_probability(confidence),
                confidence_source="MANAGEMENT",
                value_impact_pct=vi_pct,
                status=status,
                notes=_safe_desc(desc, "SEC filing")[:300] if desc != item_name else None,
            ):
                count += 1

    log.info("sec_material_facts -> company_milestones: {n} inserted", n=count)
    return count


# ── Source 4: earnings_analysis ───────────────────────────────────────

def _from_earnings(engine: Engine) -> int:
    """Populate milestones from earnings_analysis (call tone data)."""
    count = 0
    with engine.begin() as conn:
        if not _table_exists(conn, "earnings_analysis"):
            log.debug("earnings_analysis table does not exist, skipping")
            return 0

        rows = conn.execute(text("""
            SELECT ticker, filing_date, tone_label, overall_tone,
                   tone_shift, confidence, forward_looking_count,
                   guidance_phrases
            FROM earnings_analysis
            ORDER BY filing_date DESC NULLS LAST
            LIMIT 50000
        """)).fetchall()

        for r in rows:
            ticker = r[0] or ""
            filing_date = r[1]
            tone_label = r[2] or "neutral"
            overall_tone = r[3] or 0.0
            tone_shift = r[4]
            confidence = r[5] or 0.5
            fwd_count = r[6] or 0
            r[7]  # JSONB

            ann_date = filing_date or date.today()

            # Determine direction from tone
            if overall_tone > 0.2:
                direction = "bullish"
            elif overall_tone < -0.2:
                direction = "bearish"
            else:
                direction = "neutral"

            status = _direction_to_status(direction)

            # Estimate impact from tone shift
            vi_pct = None
            if tone_shift is not None:
                vi_pct = abs(float(tone_shift)) * 5.0  # rough: 0.2 shift = 1%

            shift_note = f"Tone shift: {tone_shift:+.2f}" if tone_shift else ""
            fwd_note = f"Forward-looking: {fwd_count}" if fwd_count > 0 else ""
            notes = "; ".join(filter(None, [shift_note, fwd_note]))

            if _insert_milestone(
                conn,
                ticker=ticker,
                milestone_type="EARNINGS_GUIDANCE",
                announced_date=ann_date,
                description=f"{ticker} Earnings Call — {tone_label.title()}",
                probability=_clamp_probability(confidence),
                confidence_source="MANAGEMENT",
                value_impact_pct=vi_pct,
                status=status,
                notes=notes or None,
            ):
                count += 1

    log.info("earnings_analysis -> company_milestones: {n} inserted", n=count)
    return count


# ── Source 5: signal_data (insider + congressional trades) ────────────

def _from_signals(engine: Engine) -> int:
    """Populate milestones from signal_data (high-magnitude only)."""
    count = 0
    with engine.begin() as conn:
        if not _table_exists(conn, "signal_data"):
            log.debug("signal_data table does not exist, skipping")
            return 0

        rows = conn.execute(text("""
            SELECT signal_type, signal_date, ticker, actor,
                   direction, magnitude, description, confidence
            FROM signal_data
            WHERE magnitude > 5
              AND signal_type IN (
                  'insider', 'quiverquant:insider',
                  'congressional', 'quiverquant:house', 'quiverquant:senate'
              )
              AND ticker IS NOT NULL
            ORDER BY signal_date DESC NULLS LAST
            LIMIT 50000
        """)).fetchall()

        for r in rows:
            signal_type = r[0] or ""
            signal_date = r[1]
            ticker = r[2] or ""
            actor = r[3] or ""
            direction = (r[4] or "neutral").lower()
            magnitude = r[5] or 0.0
            desc = r[6] or ""
            confidence_label = r[7] or "derived"

            ann_date = signal_date or date.today()

            # Map signal type to milestone type
            if "insider" in signal_type.lower():
                milestone_type = "STRATEGIC"
                title = f"Insider Activity: {actor}" if actor else "Insider Activity"
                conf_src = "INSIDER"
            else:
                milestone_type = "REGULATORY"
                title = f"Congressional Trade: {actor}" if actor else "Congressional Trade"
                conf_src = "MARKET"

            # Direction
            if direction == "bullish":
                title += " (Buy)"
            elif direction == "bearish":
                title += " (Sell)"

            status = _direction_to_status(direction)

            # Map confidence label to probability
            conf_map = {
                "confirmed": 0.9,
                "derived": 0.6,
                "estimated": 0.5,
                "rumored": 0.3,
                "inferred": 0.4,
            }
            prob = conf_map.get(confidence_label, 0.5)

            # Magnitude to impact
            vi_pct = float(magnitude) * 0.3 if magnitude else None

            if _insert_milestone(
                conn,
                ticker=ticker,
                milestone_type=milestone_type,
                announced_date=ann_date,
                description=_safe_desc(title),
                probability=prob,
                confidence_source=conf_src,
                value_impact_pct=vi_pct,
                status=status,
                notes=_safe_desc(desc, "Signal")[:300] if desc else None,
            ):
                count += 1

    log.info("signal_data -> company_milestones: {n} inserted", n=count)
    return count


# ── Source 6: oracle_predictions ──────────────────────────────────────

def _from_oracle(engine: Engine) -> int:
    """Populate milestones from oracle_predictions (high confidence)."""
    count = 0
    with engine.begin() as conn:
        if not _table_exists(conn, "oracle_predictions"):
            log.debug("oracle_predictions table does not exist, skipping")
            return 0

        rows = conn.execute(text("""
            SELECT ticker, prediction_type, direction, target_price,
                   entry_price, expiry, confidence, expected_move_pct,
                   model_name, created_at
            FROM oracle_predictions
            WHERE confidence > 0.7
            ORDER BY created_at DESC NULLS LAST
            LIMIT 50000
        """)).fetchall()

        for r in rows:
            ticker = r[0] or ""
            pred_type = r[1] or "directional"
            direction = (r[2] or "neutral").lower()
            target_price = r[3]
            entry_price = r[4]
            expiry = r[5]
            confidence = r[6] or 0.7
            expected_move = r[7]
            model_name = r[8] or "oracle"
            created_at = r[9]

            ann_date = _extract_date(created_at) or date.today()
            target_dt = expiry

            # Build description
            if target_price and entry_price:
                desc = (f"Oracle {direction.upper()}: "
                        f"${float(entry_price):.2f} → ${float(target_price):.2f} "
                        f"({model_name})")
            else:
                desc = f"Oracle {direction.upper()} {pred_type} ({model_name})"

            status = _direction_to_status(direction)
            vi_pct = abs(float(expected_move)) if expected_move else None

            if _insert_milestone(
                conn,
                ticker=ticker,
                milestone_type="STRATEGIC",
                announced_date=ann_date,
                description=_safe_desc(desc),
                probability=_clamp_probability(confidence),
                confidence_source="CALCULATED",
                value_impact_pct=vi_pct,
                status=status,
                target_date=target_dt,
                notes=f"Model: {model_name}; Confidence: {confidence:.2f}",
            ):
                count += 1

    log.info("oracle_predictions -> company_milestones: {n} inserted", n=count)
    return count


# ── Source 7: catalyst_calendar ───────────────────────────────────────

def _from_catalyst_calendar(engine: Engine) -> int:
    """Populate milestones from catalyst_calendar (trials, FDA)."""
    count = 0
    with engine.begin() as conn:
        if not _table_exists(conn, "catalyst_calendar"):
            log.debug("catalyst_calendar table does not exist, skipping")
            return 0

        rows = conn.execute(text("""
            SELECT ticker, nct_id, event_type, expected_date,
                   confidence_window_days, source, notes
            FROM catalyst_calendar
            WHERE is_active = TRUE
            ORDER BY expected_date NULLS LAST
            LIMIT 50000
        """)).fetchall()

        for r in rows:
            ticker = r[0] or ""
            nct_id = r[1] or ""
            event_type = (r[2] or "CATALYST").upper()
            expected_date = r[3]
            window_days = r[4] or 30
            source = r[5] or ""
            notes_text = r[6] or ""

            ann_date = expected_date or date.today()

            # Map event type to milestone type
            if "FDA" in event_type:
                milestone_type = "REGULATORY"
            elif event_type in ("READOUT", "TOPLINE", "DATA"):
                milestone_type = "PRODUCT_LAUNCH"
            else:
                milestone_type = "REGULATORY"

            desc = f"{event_type.replace('_', ' ').title()}: {nct_id}" if nct_id else event_type.replace("_", " ").title()

            # Wider window = lower confidence
            if window_days <= 14:
                prob = 0.75
            elif window_days <= 30:
                prob = 0.60
            else:
                prob = 0.45

            if _insert_milestone(
                conn,
                ticker=ticker,
                milestone_type=milestone_type,
                announced_date=ann_date,
                description=_safe_desc(desc),
                probability=prob,
                confidence_source="ANALYST",
                target_date=expected_date,
                notes=f"Window: ±{window_days}d; Source: {source}; {notes_text}".strip("; "),
            ):
                count += 1

    log.info("catalyst_calendar -> company_milestones: {n} inserted", n=count)
    return count


# ── Helpers ───────────────────────────────────────────────────────────

def _extract_date(val: Any) -> date | None:
    """Extract a date from a datetime, date, or string."""
    if val is None:
        return None
    if isinstance(val, date) and not isinstance(val, datetime):
        return val
    if isinstance(val, datetime):
        return val.date()
    try:
        return datetime.fromisoformat(str(val)[:10]).date()
    except (ValueError, TypeError):
        return None


# ── Main ──────────────────────────────────────────────────────────────

_SOURCE_MAP: dict[str, Any] = {
    "business_events": _from_business_events,
    "deals": _from_deal_pipeline,
    "sec": _from_sec_facts,
    "earnings": _from_earnings,
    "signals": _from_signals,
    "oracle": _from_oracle,
    "catalyst": _from_catalyst_calendar,
}


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Populate company_milestones from intelligence sources")
    parser.add_argument("--dry-run", action="store_true", help="Count rows only, no writes")
    parser.add_argument("--source", type=str, default=None,
                        help=f"Run a single source: {', '.join(_SOURCE_MAP.keys())}")
    args = parser.parse_args()

    from db import get_engine

    engine = get_engine()

    if args.dry_run:
        log.info("DRY RUN — counting available rows only")
        with engine.connect() as conn:
            for tbl in ("business_events", "deal_pipeline", "sec_material_facts",
                        "earnings_analysis", "signal_data", "oracle_predictions",
                        "catalyst_calendar"):
                if _table_exists(conn, tbl):
                    n = conn.execute(text(f"SELECT COUNT(*) FROM {tbl}")).scalar()  # noqa: S608
                    log.info("  {tbl}: {n:,} rows", tbl=tbl, n=n)
                else:
                    log.info("  {tbl}: DOES NOT EXIST", tbl=tbl)

            if _table_exists(conn, "company_milestones"):
                n = conn.execute(text("SELECT COUNT(*) FROM company_milestones")).scalar()
                log.info("  company_milestones (current): {n:,} rows", n=n)
        return

    _ensure_table(engine)

    total = 0

    if args.source:
        fn = _SOURCE_MAP.get(args.source)
        if fn is None:
            log.error("Unknown source: {s}. Valid: {v}",
                      s=args.source, v=", ".join(_SOURCE_MAP.keys()))
            sys.exit(1)
        total += fn(engine)
    else:
        total += _from_business_events(engine)
        total += _from_deal_pipeline(engine)
        total += _from_sec_facts(engine)
        total += _from_earnings(engine)
        total += _from_signals(engine)
        total += _from_oracle(engine)
        total += _from_catalyst_calendar(engine)

    # Final count
    with engine.connect() as conn:
        final = conn.execute(text("SELECT COUNT(*) FROM company_milestones")).scalar()

    log.info("Milestones populated: {n} new, {total} total in table",
             n=total, total=final)


if __name__ == "__main__":
    main()
