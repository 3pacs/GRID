"""
GRID Intelligence — Lever Puller Identification & Tracking.

Identifies WHO moves markets, tracks their actions, and models their
motivations. A "lever puller" is anyone whose actions reliably precede
significant price moves.

Categories:
  1. Fed officials   — speeches, dot plot votes, dissents
  2. Congressional   — trading disclosures (ingested via congressional.py)
  3. Corporate insiders — Form 4 filings (ingested via insider_filings.py)
  4. Institutional allocators — 13F filers, ETF flow drivers
  5. Market makers   — dealer positioning (GEX regime)

For each lever puller we model:
  - Position:   What power do they have?
  - History:    When they acted before, what happened to prices?
  - Motivation: Are they acting in self-interest or institutional mandate?
  - Timing:     How far ahead of the move do they act?
  - Confidence: Bayesian score from trust_scorer

Key entry points:
  identify_lever_pullers   — rank top 50 across all categories
  assess_motivation        — rule-based + optional LLM motivation assessment
  get_active_lever_events  — recent actions with motivation context
  find_lever_convergence   — multiple pullers on the same ticker = high conviction
  generate_lever_report    — full narrative report
  get_lever_context_for_ticker — per-ticker detail for watchlist pages
"""

from __future__ import annotations

import json
import math
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Constants ─────────────────────────────────────────────────────────────

# Influence weights by category (how much market impact a source class has)
INFLUENCE_WEIGHTS: dict[str, float] = {
    "fed": 1.0,
    "congress": 0.5,
    "insider": 0.6,
    "institutional": 0.7,
    "dealer": 0.8,
    "foreign_lobbying": 0.6,   # FARA-registered foreign agents
    "geopolitical": 0.7,       # GDELT tension/actor signals
    "diplomatic_cable": 0.4,   # Declassified FOIA cables (lagged but contextual)
}

# Higher-influence congressional positions (committee chairs, leadership)
CONGRESS_LEADERSHIP_KEYWORDS: set[str] = {
    "chair", "chairman", "chairwoman", "ranking member",
    "speaker", "majority leader", "minority leader", "whip",
}

CONGRESS_FINANCIAL_COMMITTEES: set[str] = {
    "financial services", "banking", "finance", "ways and means",
    "appropriations", "budget", "commerce", "energy",
    "armed services", "intelligence", "homeland security",
}

# Sector-to-committee jurisdiction mapping for motivation assessment
SECTOR_COMMITTEE_MAP: dict[str, set[str]] = {
    "XLK": {"commerce", "intelligence", "science"},
    "XLF": {"financial services", "banking", "finance"},
    "XLE": {"energy", "natural resources"},
    "XLV": {"health", "finance"},
    "XLI": {"armed services", "transportation", "infrastructure"},
    "XLB": {"natural resources", "energy"},
    "XLRE": {"financial services", "banking"},
    "XLU": {"energy", "commerce"},
    "XLC": {"commerce", "intelligence"},
    "XLY": {"commerce", "finance"},
    "XLP": {"agriculture", "commerce"},
}

# Ticker-to-sector ETF rough mapping (extend as needed)
TICKER_SECTOR_HINTS: dict[str, str] = {
    "AAPL": "XLK", "MSFT": "XLK", "GOOGL": "XLC", "AMZN": "XLY",
    "META": "XLC", "NVDA": "XLK", "TSLA": "XLY", "JPM": "XLF",
    "BAC": "XLF", "GS": "XLF", "XOM": "XLE", "CVX": "XLE",
    "JNJ": "XLV", "PFE": "XLV", "UNH": "XLV", "LMT": "XLI",
    "RTX": "XLI", "BA": "XLI", "GD": "XLI", "NOC": "XLI",
}

# Maximum lever pullers to return from identification
MAX_LEVER_PULLERS: int = 50

# Default lookback for active events
DEFAULT_EVENT_LOOKBACK_DAYS: int = 30

# Convergence detection thresholds
CONVERGENCE_MIN_PULLERS: int = 2
CONVERGENCE_WINDOW_DAYS: int = 14


# ── Data Classes ──────────────────────────────────────────────────────────

@dataclass
class LeverPuller:
    """Profile of a person or institution whose actions predict market moves."""

    id: str
    name: str
    category: str               # fed, congress, insider, institutional, dealer
    influence_rank: float       # 0-1, how much market impact
    trust_score: float          # from trust_scorer
    position: str               # "Fed Governor", "House Financial Services Committee", etc.
    motivation_model: str       # "self_serving", "institutional_mandate", "hedging", "unknown"
    recent_actions: list[dict] = field(default_factory=list)
    avg_lead_time_days: float = 0.0
    best_calls: list[dict] = field(default_factory=list)
    worst_calls: list[dict] = field(default_factory=list)


@dataclass
class LeverEvent:
    """A specific action taken by a lever puller."""

    puller: LeverPuller
    action: str                 # "BUY", "SELL", "SPEECH_HAWKISH", "SPEECH_DOVISH", "POSITION_INCREASE"
    tickers: list[str]
    timestamp: str
    motivation_assessment: str  # "Likely informed — committee overlap", "Routine rebalancing", etc.
    confidence: float
    context: str                # what was happening in the market at the time


# ── Table Setup ───────────────────────────────────────────────────────────

def _ensure_lever_table(engine: Engine) -> None:
    """Create the lever_pullers table if it does not exist.

    Parameters:
        engine: SQLAlchemy engine connected to the GRID database.
    """
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS lever_pullers (
                id              SERIAL PRIMARY KEY,
                source_type     TEXT NOT NULL,
                source_id       TEXT NOT NULL UNIQUE,
                name            TEXT NOT NULL,
                category        TEXT NOT NULL,
                position        TEXT,
                influence_rank  NUMERIC DEFAULT 0.5,
                motivation_model TEXT DEFAULT 'unknown',
                trust_score     NUMERIC DEFAULT 0.5,
                avg_lead_time_days NUMERIC,
                total_signals   INT DEFAULT 0,
                correct_signals INT DEFAULT 0,
                metadata        JSONB,
                updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_lever_pullers_category
                ON lever_pullers (category)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_lever_pullers_influence
                ON lever_pullers (influence_rank DESC)
        """))
    log.debug("lever_pullers table ensured")


# ── Helpers ───────────────────────────────────────────────────────────────

def _category_from_source_type(source_type: str) -> str:
    """Map signal_sources.source_type to lever puller category.

    Parameters:
        source_type: The source_type value from signal_sources.

    Returns:
        Normalised category string.
    """
    mapping = {
        "congressional": "congress",
        "insider": "insider",
        "darkpool": "dealer",
        "social": "institutional",
        "scanner": "dealer",
        "fed": "fed",
        "institutional": "institutional",
    }
    return mapping.get(source_type.lower(), "unknown")


def _influence_for_source(
    source_type: str,
    source_id: str,
    metadata: dict[str, Any] | None = None,
) -> float:
    """Compute influence weight for a source.

    Committee chairs and leadership get boosted influence within their
    category. Fed officials always get the maximum weight.

    Parameters:
        source_type: Source type from signal_sources.
        source_id: Source identifier (name).
        metadata: Optional metadata dict with committee/title info.

    Returns:
        Influence rank between 0 and 1.
    """
    category = _category_from_source_type(source_type)
    base = INFLUENCE_WEIGHTS.get(category, 0.3)

    if metadata is None:
        return base

    # Boost congressional leadership
    if category == "congress":
        committee = (metadata.get("committee") or "").lower()
        position_str = (metadata.get("position") or "").lower()

        # Financial committee members get a bump
        for kw in CONGRESS_FINANCIAL_COMMITTEES:
            if kw in committee:
                base = max(base, 0.7)
                break

        # Leadership titles get a larger bump
        for kw in CONGRESS_LEADERSHIP_KEYWORDS:
            if kw in position_str or kw in committee:
                base = max(base, 0.8)
                break

    # Boost C-suite insiders over 10% owners
    if category == "insider":
        title = (metadata.get("insider_title") or "").lower()
        if any(t in title for t in ("ceo", "cfo", "coo", "president", "director")):
            base = max(base, 0.7)

    return min(base, 1.0)


def _position_label(
    source_type: str,
    source_id: str,
    metadata: dict[str, Any] | None = None,
) -> str:
    """Build a human-readable position label for a lever puller.

    Parameters:
        source_type: Source type from signal_sources.
        source_id: Source identifier.
        metadata: Optional metadata dict.

    Returns:
        Human-readable position string.
    """
    if metadata is None:
        metadata = {}

    category = _category_from_source_type(source_type)

    if category == "congress":
        chamber = metadata.get("chamber", "Congress")
        committee = metadata.get("committee", "")
        state = metadata.get("state", "")
        parts = [chamber]
        if committee:
            parts.append(committee)
        if state:
            parts.append(f"({state})")
        return " — ".join(parts) if len(parts) > 1 else parts[0]

    if category == "insider":
        title = metadata.get("insider_title", "")
        ticker = metadata.get("ticker", "")
        if title and ticker:
            return f"{title} of {ticker}"
        return title or f"Insider at {ticker}" if ticker else "Corporate Insider"

    if category == "fed":
        return metadata.get("title", "Fed Official")

    if category == "dealer":
        return "Market Maker / Dealer"

    if category == "institutional":
        return metadata.get("fund_name", "Institutional Allocator")

    return "Unknown"


def _get_sector_for_ticker(ticker: str) -> str | None:
    """Return sector ETF symbol for a ticker, if known.

    Parameters:
        ticker: Stock ticker symbol.

    Returns:
        Sector ETF symbol or None.
    """
    return TICKER_SECTOR_HINTS.get(ticker.upper())


def _committee_has_jurisdiction(committee: str, ticker: str) -> bool:
    """Check if a congressional committee has jurisdiction over a ticker's sector.

    Parameters:
        committee: Committee name string.
        ticker: Stock ticker symbol.

    Returns:
        True if there is a plausible jurisdictional overlap.
    """
    sector = _get_sector_for_ticker(ticker)
    if not sector:
        return False

    relevant_committees = SECTOR_COMMITTEE_MAP.get(sector, set())
    committee_lower = committee.lower()

    return any(kw in committee_lower for kw in relevant_committees)


# ── 1. Identify Lever Pullers ────────────────────────────────────────────

def identify_lever_pullers(engine: Engine) -> list[LeverPuller]:
    """Query signal_sources, aggregate by source, and rank by trust x influence.

    Scans all scored signals in signal_sources, groups by (source_type,
    source_id), computes trust scores and influence ranks, and returns the
    top MAX_LEVER_PULLERS lever pullers across all categories.

    Parameters:
        engine: SQLAlchemy engine connected to the GRID database.

    Returns:
        List of LeverPuller instances, sorted by trust_score * influence_rank descending.
    """
    _ensure_lever_table(engine)

    pullers: list[LeverPuller] = []

    with engine.connect() as conn:
        # Aggregate scored signals by source
        rows = conn.execute(text("""
            SELECT
                source_type,
                source_id,
                COUNT(*) AS total_signals,
                SUM(CASE WHEN outcome = 'CORRECT' THEN 1 ELSE 0 END) AS correct,
                SUM(CASE WHEN outcome = 'WRONG' THEN 1 ELSE 0 END) AS wrong,
                AVG(CASE WHEN outcome = 'CORRECT' THEN outcome_return END) AS avg_return_correct,
                AVG(CASE WHEN outcome = 'WRONG' THEN outcome_return END) AS avg_return_wrong,
                MAX(signal_date) AS last_signal_date,
                AVG(avg_lead_time_hours) AS avg_lead_hours,
                AVG(trust_score) AS avg_trust
            FROM signal_sources
            WHERE outcome IN ('CORRECT', 'WRONG')
            GROUP BY source_type, source_id
            ORDER BY AVG(trust_score) DESC
            LIMIT :lim
        """), {"lim": MAX_LEVER_PULLERS * 3}).fetchall()

        if not rows:
            log.info("No scored sources found for lever puller identification")
            return []

        # Fetch the most recent metadata for each source (for position/committee info)
        for r in rows:
            src_type, src_id = r[0], r[1]
            total, correct, wrong = int(r[2]), int(r[3] or 0), int(r[4] or 0)
            avg_ret_correct = float(r[5]) if r[5] is not None else 0.0
            avg_ret_wrong = float(r[6]) if r[6] is not None else 0.0
            last_date = r[7]
            avg_lead_hours = float(r[8]) if r[8] is not None else 0.0
            avg_trust = float(r[9]) if r[9] is not None else 0.5

            # Fetch latest signal metadata for context
            meta_row = conn.execute(text("""
                SELECT signal_value FROM signal_sources
                WHERE source_type = :st AND source_id = :si
                ORDER BY signal_date DESC
                LIMIT 1
            """), {"st": src_type, "si": src_id}).fetchone()

            metadata: dict[str, Any] = {}
            if meta_row and meta_row[0]:
                try:
                    metadata = json.loads(meta_row[0]) if isinstance(meta_row[0], str) else meta_row[0]
                except (json.JSONDecodeError, TypeError):
                    pass

            category = _category_from_source_type(src_type)
            influence = _influence_for_source(src_type, src_id, metadata)
            position = _position_label(src_type, src_id, metadata)

            # Bayesian trust: (hits + 1) / (hits + misses + 2)
            trust = (correct + 1.0) / (correct + wrong + 2.0)

            # Fetch best and worst calls
            best_calls = _fetch_top_calls(conn, src_type, src_id, "CORRECT", limit=3)
            worst_calls = _fetch_top_calls(conn, src_type, src_id, "WRONG", limit=3)

            # Fetch recent actions
            recent_actions = _fetch_recent_actions(conn, src_type, src_id, limit=5)

            avg_lead_days = avg_lead_hours / 24.0 if avg_lead_hours else 0.0

            puller = LeverPuller(
                id=f"{src_type}:{src_id}",
                name=src_id,
                category=category,
                influence_rank=influence,
                trust_score=round(trust, 4),
                position=position,
                motivation_model="unknown",
                recent_actions=recent_actions,
                avg_lead_time_days=round(avg_lead_days, 2),
                best_calls=best_calls,
                worst_calls=worst_calls,
            )
            pullers.append(puller)

    # Sort by composite score: trust * influence
    pullers.sort(key=lambda p: p.trust_score * p.influence_rank, reverse=True)
    pullers = pullers[:MAX_LEVER_PULLERS]

    # Persist to lever_pullers table
    _persist_lever_pullers(engine, pullers)

    log.info(
        "Identified {n} lever pullers (top composite: {top})",
        n=len(pullers),
        top=f"{pullers[0].name} ({pullers[0].trust_score:.3f})" if pullers else "none",
    )
    return pullers


def _fetch_top_calls(
    conn: Any,
    source_type: str,
    source_id: str,
    outcome: str,
    limit: int = 3,
) -> list[dict]:
    """Fetch the top calls for a source by outcome return magnitude.

    Parameters:
        conn: Active database connection.
        source_type: Source type.
        source_id: Source identifier.
        outcome: 'CORRECT' or 'WRONG'.
        limit: Maximum number of calls to return.

    Returns:
        List of dicts with ticker, signal_date, outcome_return, signal_type.
    """
    order = "DESC" if outcome == "CORRECT" else "ASC"
    rows = conn.execute(text(f"""
        SELECT ticker, signal_date, outcome_return, signal_type
        FROM signal_sources
        WHERE source_type = :st AND source_id = :si AND outcome = :oc
          AND outcome_return IS NOT NULL
        ORDER BY outcome_return {order}
        LIMIT :lim
    """), {"st": source_type, "si": source_id, "oc": outcome, "lim": limit}).fetchall()

    return [
        {
            "ticker": r[0],
            "signal_date": r[1].isoformat() if hasattr(r[1], "isoformat") else str(r[1]),
            "outcome_return": float(r[2]) if r[2] is not None else 0.0,
            "signal_type": r[3],
        }
        for r in rows
    ]


def _fetch_recent_actions(
    conn: Any,
    source_type: str,
    source_id: str,
    limit: int = 5,
) -> list[dict]:
    """Fetch the most recent actions for a source.

    Parameters:
        conn: Active database connection.
        source_type: Source type.
        source_id: Source identifier.
        limit: Maximum number of actions.

    Returns:
        List of dicts with ticker, signal_date, signal_type, signal_value.
    """
    rows = conn.execute(text("""
        SELECT ticker, signal_date, signal_type, signal_value
        FROM signal_sources
        WHERE source_type = :st AND source_id = :si
        ORDER BY signal_date DESC
        LIMIT :lim
    """), {"st": source_type, "si": source_id, "lim": limit}).fetchall()

    actions = []
    for r in rows:
        val = {}
        if r[3]:
            try:
                val = json.loads(r[3]) if isinstance(r[3], str) else r[3]
            except (json.JSONDecodeError, TypeError):
                pass
        actions.append({
            "ticker": r[0],
            "signal_date": r[1].isoformat() if hasattr(r[1], "isoformat") else str(r[1]),
            "signal_type": r[2],
            "details": val,
        })
    return actions


def _persist_lever_pullers(engine: Engine, pullers: list[LeverPuller]) -> None:
    """Upsert identified lever pullers into the lever_pullers table.

    Parameters:
        engine: SQLAlchemy engine.
        pullers: List of LeverPuller dataclass instances.
    """
    with engine.begin() as conn:
        for p in pullers:
            src_type = p.id.split(":")[0] if ":" in p.id else p.category
            conn.execute(text("""
                INSERT INTO lever_pullers
                    (source_type, source_id, name, category, position,
                     influence_rank, motivation_model, trust_score,
                     avg_lead_time_days, total_signals, correct_signals,
                     metadata, updated_at)
                VALUES
                    (:stype, :sid, :name, :cat, :pos,
                     :inf, :mot, :trust,
                     :lead, :total, :correct,
                     :meta, NOW())
                ON CONFLICT (source_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    category = EXCLUDED.category,
                    position = EXCLUDED.position,
                    influence_rank = EXCLUDED.influence_rank,
                    motivation_model = EXCLUDED.motivation_model,
                    trust_score = EXCLUDED.trust_score,
                    avg_lead_time_days = EXCLUDED.avg_lead_time_days,
                    total_signals = EXCLUDED.total_signals,
                    correct_signals = EXCLUDED.correct_signals,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW()
            """), {
                "stype": src_type,
                "sid": p.id,
                "name": p.name,
                "cat": p.category,
                "pos": p.position,
                "inf": p.influence_rank,
                "mot": p.motivation_model,
                "trust": p.trust_score,
                "lead": p.avg_lead_time_days,
                "total": len(p.recent_actions) + len(p.best_calls) + len(p.worst_calls),
                "correct": len(p.best_calls),
                "meta": json.dumps({
                    "best_calls": p.best_calls,
                    "worst_calls": p.worst_calls,
                }),
            })

    log.debug("Persisted {n} lever pullers to database", n=len(pullers))


# ── 2. Assess Motivation ─────────────────────────────────────────────────

def assess_motivation(
    puller: LeverPuller,
    action: dict[str, Any],
    engine: Engine,
) -> str:
    """Assess the likely motivation behind a lever puller's action.

    Rule-based assessment with category-specific logic:
    - Congressional: committee jurisdiction overlap = likely informed
    - Insider: scheduled 10b5-1 plan = routine; discretionary = informed
    - Institutional: deviation from stated strategy = notable
    - Fed: policy action = institutional mandate
    - Dealer: hedging by definition

    Falls back to LLM for nuanced cases if available.

    Parameters:
        puller: The LeverPuller taking the action.
        action: Dict with ticker, signal_type, details, signal_date.
        engine: SQLAlchemy engine for additional lookups.

    Returns:
        Motivation string: "likely_informed", "routine", "hedging",
        "contrarian", "unknown".
    """
    ticker = action.get("ticker", "")
    signal_type = action.get("signal_type", "")
    details = action.get("details", {})

    # ── Congressional motivation ──
    if puller.category == "congress":
        committee = details.get("committee", "")
        if committee and ticker and _committee_has_jurisdiction(committee, ticker):
            return "likely_informed"

        # Large amount in a narrow sector = suspicious
        amount_mid = details.get("amount_midpoint", 0)
        if amount_mid and float(amount_mid) > 250_000:
            return "likely_informed"

        # Short disclosure lag (acted and disclosed quickly) = more routine
        lag = details.get("disclosure_lag_days")
        if lag is not None and int(lag) < 10:
            return "routine"

        return "unknown"

    # ── Insider motivation ──
    if puller.category == "insider":
        # Buys are almost always informed (insiders buy for one reason)
        if signal_type in ("BUY", "UNUSUAL_BUY"):
            return "likely_informed"

        # Large unusual sells = informed or hedging
        is_unusual = details.get("is_unusual_size", False)
        if is_unusual and signal_type in ("SELL", "UNUSUAL_SELL"):
            return "hedging"

        # Derivative transactions often routine
        if details.get("is_derivative", False):
            return "routine"

        # Default sells to routine (many reasons to sell)
        if "SELL" in signal_type:
            return "routine"

        return "unknown"

    # ── Fed motivation ──
    if puller.category == "fed":
        return "institutional_mandate"

    # ── Dealer motivation ──
    if puller.category == "dealer":
        return "hedging"

    # ── Institutional motivation ──
    if puller.category == "institutional":
        # Check if action aligns with recent trend or is contrarian
        return _assess_institutional_motivation(puller, action, engine)

    return "unknown"


def _assess_institutional_motivation(
    puller: LeverPuller,
    action: dict[str, Any],
    engine: Engine,
) -> str:
    """Determine if an institutional action is routine or a deviation.

    Checks whether the action direction aligns with the majority of this
    puller's recent actions. A deviation from pattern is more notable.

    Parameters:
        puller: The institutional LeverPuller.
        action: The current action dict.
        engine: SQLAlchemy engine.

    Returns:
        Motivation string.
    """
    signal_type = action.get("signal_type", "")

    # Count recent action directions
    buy_count = sum(
        1 for a in puller.recent_actions
        if "BUY" in a.get("signal_type", "")
    )
    sell_count = sum(
        1 for a in puller.recent_actions
        if "SELL" in a.get("signal_type", "")
    )

    total = buy_count + sell_count
    if total == 0:
        return "unknown"

    # If mostly buying and this is a sell (or vice versa), it is contrarian
    if "BUY" in signal_type and sell_count > buy_count:
        return "contrarian"
    if "SELL" in signal_type and buy_count > sell_count:
        return "contrarian"

    return "routine"


# ── 3. Get Active Lever Events ───────────────────────────────────────────

def get_active_lever_events(
    engine: Engine,
    days: int = DEFAULT_EVENT_LOOKBACK_DAYS,
) -> list[LeverEvent]:
    """Fetch recent actions from identified lever pullers.

    Queries signal_sources for actions within the lookback window,
    matches them to known lever pullers, assesses motivation, and
    sorts by confidence x influence.

    Parameters:
        engine: SQLAlchemy engine.
        days: Number of days to look back (default 30).

    Returns:
        List of LeverEvent instances sorted by confidence * influence descending.
    """
    _ensure_lever_table(engine)

    # First, get known lever pullers
    pullers = identify_lever_pullers(engine)
    puller_map: dict[str, LeverPuller] = {p.id: p for p in pullers}

    cutoff = date.today() - timedelta(days=days)
    events: list[LeverEvent] = []

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT source_type, source_id, ticker, signal_date,
                   signal_type, signal_value, trust_score
            FROM signal_sources
            WHERE signal_date >= :cutoff
            ORDER BY signal_date DESC
        """), {"cutoff": cutoff}).fetchall()

        for r in rows:
            src_type, src_id, ticker, sig_date, sig_type, sig_val, trust = r
            puller_id = f"{src_type}:{src_id}"
            puller = puller_map.get(puller_id)

            if puller is None:
                continue

            # Parse signal value metadata
            details: dict[str, Any] = {}
            if sig_val:
                try:
                    details = json.loads(sig_val) if isinstance(sig_val, str) else sig_val
                except (json.JSONDecodeError, TypeError):
                    pass

            action_dict = {
                "ticker": ticker,
                "signal_type": sig_type,
                "details": details,
                "signal_date": sig_date.isoformat() if hasattr(sig_date, "isoformat") else str(sig_date),
            }

            motivation = assess_motivation(puller, action_dict, engine)

            # Build motivation assessment narrative
            motivation_text = _motivation_narrative(motivation, puller, ticker, details)

            confidence = float(trust) if trust else puller.trust_score

            event = LeverEvent(
                puller=puller,
                action=sig_type,
                tickers=[ticker] if ticker else [],
                timestamp=sig_date.isoformat() if hasattr(sig_date, "isoformat") else str(sig_date),
                motivation_assessment=motivation_text,
                confidence=round(confidence, 4),
                context=_build_event_context(details),
            )
            events.append(event)

    # Sort by confidence * influence
    events.sort(
        key=lambda e: e.confidence * e.puller.influence_rank,
        reverse=True,
    )

    log.info(
        "Found {n} active lever events in the last {d} days",
        n=len(events),
        d=days,
    )
    return events


def _motivation_narrative(
    motivation: str,
    puller: LeverPuller,
    ticker: str,
    details: dict[str, Any],
) -> str:
    """Build a human-readable motivation assessment string.

    Parameters:
        motivation: Raw motivation classification.
        puller: The lever puller.
        ticker: Ticker involved.
        details: Signal metadata.

    Returns:
        Narrative motivation string.
    """
    if motivation == "likely_informed":
        if puller.category == "congress":
            committee = details.get("committee", "")
            if committee:
                return f"Likely informed — {committee} has jurisdiction over {ticker}'s sector"
            return f"Likely informed — large position or unusual timing"
        if puller.category == "insider":
            return "Likely informed — insider buying own stock (skin in the game)"
        return "Likely informed — unusual pattern"

    if motivation == "routine":
        if puller.category == "insider":
            return "Routine — likely scheduled 10b5-1 plan or diversification"
        return "Routine rebalancing"

    if motivation == "hedging":
        return "Hedging — position management or risk reduction"

    if motivation == "contrarian":
        return "Contrarian — acting against their recent pattern"

    if motivation == "institutional_mandate":
        return "Institutional mandate — policy role"

    return "Unknown motivation — insufficient data"


def _build_event_context(details: dict[str, Any]) -> str:
    """Build a brief context string from signal details.

    Parameters:
        details: Signal value metadata dict.

    Returns:
        Human-readable context string.
    """
    parts: list[str] = []

    if "amount_range" in details:
        parts.append(f"Amount: {details['amount_range']}")
    if "value" in details:
        val = details["value"]
        if isinstance(val, (int, float)) and val > 0:
            parts.append(f"Value: ${val:,.0f}")
    if "shares" in details:
        shares = details["shares"]
        if isinstance(shares, (int, float)) and shares > 0:
            parts.append(f"Shares: {shares:,.0f}")
    if "insider_count" in details:
        parts.append(f"Cluster: {details['insider_count']} insiders")
    if "chamber" in details:
        parts.append(f"Chamber: {details['chamber']}")

    return "; ".join(parts) if parts else "No additional context"


# ── 4. Find Lever Convergence ────────────────────────────────────────────

def find_lever_convergence(engine: Engine) -> list[dict]:
    """Detect when multiple lever pullers act on the same ticker simultaneously.

    This is the highest-conviction signal the system can produce. When 2+
    independent lever pullers (different categories or individuals) act on
    the same ticker within CONVERGENCE_WINDOW_DAYS, it indicates strong
    consensus among informed actors.

    Results are weighted by trust score and influence rank.

    Parameters:
        engine: SQLAlchemy engine.

    Returns:
        List of convergence event dicts sorted by combined_weight descending.
    """
    _ensure_lever_table(engine)

    cutoff = date.today() - timedelta(days=CONVERGENCE_WINDOW_DAYS)

    pullers = identify_lever_pullers(engine)
    puller_map: dict[str, LeverPuller] = {p.id: p for p in pullers}

    convergences: list[dict] = []

    with engine.connect() as conn:
        # Get recent signals from known lever pullers, grouped by ticker
        rows = conn.execute(text("""
            SELECT ticker, source_type, source_id, signal_type,
                   signal_date, trust_score, signal_value
            FROM signal_sources
            WHERE signal_date >= :cutoff
              AND ticker IS NOT NULL
            ORDER BY ticker, signal_date DESC
        """), {"cutoff": cutoff}).fetchall()

    # Group by ticker
    ticker_actions: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        ticker, src_type, src_id, sig_type, sig_date, trust, sig_val = r
        puller_id = f"{src_type}:{src_id}"
        if puller_id not in puller_map:
            continue

        ticker_actions[ticker].append({
            "puller_id": puller_id,
            "puller": puller_map[puller_id],
            "signal_type": sig_type,
            "signal_date": sig_date,
            "trust_score": float(trust) if trust else 0.5,
            "signal_value": sig_val,
        })

    # Check each ticker for convergence
    for ticker, actions in ticker_actions.items():
        # Separate by direction
        for direction in ("BUY", "SELL"):
            dir_actions = [
                a for a in actions
                if direction in (a["signal_type"] or "").upper()
            ]

            # Count unique pullers
            unique_pullers = {a["puller_id"] for a in dir_actions}
            if len(unique_pullers) < CONVERGENCE_MIN_PULLERS:
                continue

            # Compute combined weight
            combined_weight = sum(
                a["puller"].trust_score * a["puller"].influence_rank
                for a in dir_actions
            )

            # Deduplicate pullers for display
            puller_details = []
            seen_ids: set[str] = set()
            for a in dir_actions:
                if a["puller_id"] in seen_ids:
                    continue
                seen_ids.add(a["puller_id"])
                p = a["puller"]
                puller_details.append({
                    "name": p.name,
                    "category": p.category,
                    "position": p.position,
                    "trust_score": p.trust_score,
                    "influence_rank": p.influence_rank,
                    "signal_type": a["signal_type"],
                    "signal_date": (
                        a["signal_date"].isoformat()
                        if hasattr(a["signal_date"], "isoformat")
                        else str(a["signal_date"])
                    ),
                })

            dates = [
                a["signal_date"] for a in dir_actions
                if hasattr(a["signal_date"], "isoformat")
            ]
            date_range = ""
            if dates:
                earliest = min(dates)
                latest = max(dates)
                e_str = earliest.isoformat() if hasattr(earliest, "isoformat") else str(earliest)
                l_str = latest.isoformat() if hasattr(latest, "isoformat") else str(latest)
                date_range = f"{e_str} to {l_str}"

            convergences.append({
                "ticker": ticker,
                "direction": direction,
                "puller_count": len(unique_pullers),
                "combined_weight": round(combined_weight, 4),
                "pullers": puller_details,
                "date_range": date_range,
                "summary": (
                    f"{len(unique_pullers)} lever pullers {direction.lower()}ing "
                    f"{ticker} in the same {CONVERGENCE_WINDOW_DAYS}-day window"
                ),
            })

    # Sort by combined weight
    convergences.sort(key=lambda c: c["combined_weight"], reverse=True)

    if convergences:
        log.info(
            "Found {n} lever convergence events (top: {t} with {p} pullers)",
            n=len(convergences),
            t=convergences[0]["ticker"],
            p=convergences[0]["puller_count"],
        )
    else:
        log.info("No lever convergence events detected")

    return convergences


# ── 5. Generate Lever Report ─────────────────────────────────────────────

def generate_lever_report(engine: Engine) -> str:
    """Generate a comprehensive lever puller intelligence report.

    Sections:
      1. Who's Moving — top lever pullers and recent actions
      2. What They're Doing — buying/selling patterns by category
      3. Why It Matters — motivation assessments
      4. Convergence Events — multiple pullers on the same ticker
      5. LLM Narrative (if available)

    Parameters:
        engine: SQLAlchemy engine.

    Returns:
        Formatted report string.
    """
    pullers = identify_lever_pullers(engine)
    events = get_active_lever_events(engine, days=DEFAULT_EVENT_LOOKBACK_DAYS)
    convergences = find_lever_convergence(engine)

    lines: list[str] = []
    lines.append("=" * 72)
    lines.append("GRID LEVER PULLER INTELLIGENCE REPORT")
    lines.append(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append("=" * 72)

    # ── Section 1: Who's Moving ──
    lines.append("")
    lines.append("--- WHO'S MOVING (Top Lever Pullers) ---")
    lines.append("")

    for i, p in enumerate(pullers[:10], 1):
        composite = p.trust_score * p.influence_rank
        lines.append(
            f"  {i:>2}. {p.name:<30} "
            f"[{p.category:<12}] "
            f"trust={p.trust_score:.3f}  "
            f"influence={p.influence_rank:.2f}  "
            f"composite={composite:.3f}"
        )
        lines.append(f"      Position: {p.position}")
        if p.recent_actions:
            latest = p.recent_actions[0]
            lines.append(
                f"      Latest: {latest.get('signal_type', '?')} "
                f"{latest.get('ticker', '?')} on {latest.get('signal_date', '?')}"
            )
        lines.append("")

    # ── Section 2: What They're Doing ──
    lines.append("--- WHAT THEY'RE DOING (Patterns by Category) ---")
    lines.append("")

    category_buys: dict[str, int] = defaultdict(int)
    category_sells: dict[str, int] = defaultdict(int)
    for e in events:
        if "BUY" in e.action:
            category_buys[e.puller.category] += 1
        elif "SELL" in e.action:
            category_sells[e.puller.category] += 1

    for cat in sorted(set(list(category_buys.keys()) + list(category_sells.keys()))):
        buys = category_buys.get(cat, 0)
        sells = category_sells.get(cat, 0)
        ratio = buys / max(buys + sells, 1) * 100
        lines.append(f"  {cat:<15} BUY={buys:<4} SELL={sells:<4} (buy ratio: {ratio:.0f}%)")
    lines.append("")

    # ── Section 3: Why It Matters ──
    lines.append("--- WHY IT MATTERS (Top Motivated Actions) ---")
    lines.append("")

    informed_events = [
        e for e in events
        if "Likely informed" in e.motivation_assessment
    ]
    for e in informed_events[:5]:
        lines.append(
            f"  * {e.puller.name} ({e.puller.category}): "
            f"{e.action} {', '.join(e.tickers)} — {e.motivation_assessment}"
        )
    if not informed_events:
        lines.append("  (No clearly informed actions detected in this window)")
    lines.append("")

    # ── Section 4: Convergence Events ──
    lines.append("--- CONVERGENCE EVENTS (Highest Conviction) ---")
    lines.append("")

    if convergences:
        for c in convergences[:5]:
            lines.append(f"  ** {c['summary']}")
            lines.append(f"     Combined weight: {c['combined_weight']:.3f}")
            lines.append(f"     Date range: {c['date_range']}")
            for pd_item in c["pullers"][:3]:
                lines.append(
                    f"       - {pd_item['name']} ({pd_item['category']}, "
                    f"trust={pd_item['trust_score']:.3f})"
                )
            lines.append("")
    else:
        lines.append("  (No convergence events detected)")
        lines.append("")

    # ── LLM narrative synthesis ──
    narrative = _generate_llm_narrative(pullers, events, convergences)
    if narrative:
        lines.append("--- NARRATIVE SYNTHESIS ---")
        lines.append("")
        lines.append(narrative)
        lines.append("")

    lines.append("=" * 72)
    report = "\n".join(lines)

    log.info(
        "Lever report generated: {p} pullers, {e} events, {c} convergences",
        p=len(pullers),
        e=len(events),
        c=len(convergences),
    )

    return report


def _generate_llm_narrative(
    pullers: list[LeverPuller],
    events: list[LeverEvent],
    convergences: list[dict],
) -> str:
    """Attempt to generate an LLM narrative connecting lever puller activity.

    Falls back to a structured summary if no LLM is available.

    Parameters:
        pullers: Identified lever pullers.
        events: Active lever events.
        convergences: Convergence events.

    Returns:
        Narrative string, or empty string on failure.
    """
    try:
        from ollama.client import get_client
        client = get_client()
    except Exception:
        client = None

    if client is None:
        # Rule-based fallback
        parts: list[str] = []
        if convergences:
            top = convergences[0]
            parts.append(
                f"The strongest signal is {top['summary']} "
                f"(combined weight {top['combined_weight']:.3f})."
            )

        informed = [e for e in events if "Likely informed" in e.motivation_assessment]
        if informed:
            parts.append(
                f"{len(informed)} actions flagged as likely informed across "
                f"{len({e.puller.category for e in informed})} categories."
            )

        if pullers:
            top_p = pullers[0]
            parts.append(
                f"Top lever puller: {top_p.name} ({top_p.category}, "
                f"trust={top_p.trust_score:.3f})."
            )

        return " ".join(parts) if parts else ""

    # LLM synthesis
    prompt = _build_narrative_prompt(pullers[:10], events[:20], convergences[:5])
    try:
        response = client.generate(
            model="hermes",
            prompt=prompt,
            options={"temperature": 0.4, "num_predict": 500},
        )
        return response.get("response", "").strip()
    except Exception as exc:
        log.debug("LLM narrative generation failed: {e}", e=str(exc))
        return ""


def _build_narrative_prompt(
    pullers: list[LeverPuller],
    events: list[LeverEvent],
    convergences: list[dict],
) -> str:
    """Build a prompt for LLM narrative synthesis.

    Parameters:
        pullers: Top lever pullers.
        events: Recent lever events.
        convergences: Convergence events.

    Returns:
        Prompt string for the LLM.
    """
    lines = [
        "You are a senior market intelligence analyst. Synthesize the following "
        "lever puller activity into a brief, actionable narrative (3-5 sentences). "
        "Focus on what the convergence of these actors implies for markets.",
        "",
        "TOP LEVER PULLERS:",
    ]
    for p in pullers[:5]:
        lines.append(f"- {p.name} ({p.category}, trust={p.trust_score:.3f}): {p.position}")

    lines.append("")
    lines.append("RECENT EVENTS:")
    for e in events[:10]:
        lines.append(
            f"- {e.puller.name} {e.action} {', '.join(e.tickers)} "
            f"({e.motivation_assessment})"
        )

    if convergences:
        lines.append("")
        lines.append("CONVERGENCE SIGNALS:")
        for c in convergences[:3]:
            lines.append(f"- {c['summary']}")

    lines.append("")
    lines.append("Narrative:")
    return "\n".join(lines)


# ── 6. Get Lever Context for Ticker ──────────────────────────────────────

def get_lever_context_for_ticker(engine: Engine, ticker: str) -> dict:
    """Get lever puller context for a specific ticker.

    For the watchlist detail page: who among the lever pullers has been
    active on this ticker, their motivation, and historical accuracy.

    Parameters:
        engine: SQLAlchemy engine.
        ticker: Stock ticker symbol.

    Returns:
        Dict with active_pullers, motivations, convergence, historical_accuracy.
    """
    _ensure_lever_table(engine)
    ticker = ticker.upper().strip()

    pullers = identify_lever_pullers(engine)
    puller_map: dict[str, LeverPuller] = {p.id: p for p in pullers}

    result: dict[str, Any] = {
        "ticker": ticker,
        "active_pullers": [],
        "motivations": [],
        "convergence": None,
        "historical_accuracy": {},
        "signal_count": 0,
    }

    with engine.connect() as conn:
        # Recent signals for this ticker from known lever pullers
        rows = conn.execute(text("""
            SELECT source_type, source_id, signal_type, signal_date,
                   signal_value, trust_score, outcome, outcome_return
            FROM signal_sources
            WHERE ticker = :t
            ORDER BY signal_date DESC
            LIMIT 50
        """), {"t": ticker}).fetchall()

        if not rows:
            return result

        # Aggregate by puller
        puller_signals: dict[str, list[dict]] = defaultdict(list)
        for r in rows:
            src_type, src_id, sig_type, sig_date, sig_val, trust, outcome, ret = r
            puller_id = f"{src_type}:{src_id}"

            details: dict[str, Any] = {}
            if sig_val:
                try:
                    details = json.loads(sig_val) if isinstance(sig_val, str) else sig_val
                except (json.JSONDecodeError, TypeError):
                    pass

            puller_signals[puller_id].append({
                "signal_type": sig_type,
                "signal_date": sig_date.isoformat() if hasattr(sig_date, "isoformat") else str(sig_date),
                "trust_score": float(trust) if trust else 0.5,
                "outcome": outcome,
                "outcome_return": float(ret) if ret is not None else None,
                "details": details,
            })

        result["signal_count"] = len(rows)

        for puller_id, signals in puller_signals.items():
            puller = puller_map.get(puller_id)
            if puller is None:
                continue

            # Historical accuracy on this ticker
            scored = [s for s in signals if s["outcome"] in ("CORRECT", "WRONG")]
            correct = sum(1 for s in scored if s["outcome"] == "CORRECT")
            total_scored = len(scored)

            accuracy = correct / total_scored if total_scored > 0 else None

            # Motivation for most recent action
            latest = signals[0]
            motivation = assess_motivation(puller, latest, engine)

            result["active_pullers"].append({
                "name": puller.name,
                "category": puller.category,
                "position": puller.position,
                "trust_score": puller.trust_score,
                "influence_rank": puller.influence_rank,
                "signals_on_ticker": len(signals),
                "accuracy_on_ticker": round(accuracy, 4) if accuracy is not None else None,
                "latest_action": latest["signal_type"],
                "latest_date": latest["signal_date"],
            })

            result["motivations"].append({
                "puller": puller.name,
                "motivation": motivation,
                "narrative": _motivation_narrative(motivation, puller, ticker, latest.get("details", {})),
            })

        # Check for convergence on this ticker specifically
        convergences = find_lever_convergence(engine)
        ticker_convergences = [c for c in convergences if c["ticker"] == ticker]
        if ticker_convergences:
            result["convergence"] = ticker_convergences[0]

        # Overall historical accuracy across all pullers on this ticker
        all_scored = [
            s for sigs in puller_signals.values() for s in sigs
            if s["outcome"] in ("CORRECT", "WRONG")
        ]
        total_correct = sum(1 for s in all_scored if s["outcome"] == "CORRECT")
        total_all = len(all_scored)
        result["historical_accuracy"] = {
            "total_signals": total_all,
            "correct": total_correct,
            "accuracy": round(total_correct / total_all, 4) if total_all > 0 else None,
        }

    log.info(
        "Lever context for {t}: {n} active pullers, {s} total signals",
        t=ticker,
        n=len(result["active_pullers"]),
        s=result["signal_count"],
    )

    return result
