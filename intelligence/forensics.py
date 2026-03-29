"""
GRID Intelligence — Forensic Analyzer.

Given a significant price move, looks backward and reconstructs what events
preceded it and why. Answers the question: "What did the market know before
this move, and who knew it?"

Pipeline:
  1. analyze_move             — full forensic report for a single price move
  2. find_significant_moves   — scan for daily moves exceeding a threshold
  3. batch_forensics          — analyze all significant moves for a ticker
  4. generate_forensic_summary — LLM synthesis across all forensic reports

Uses:
  - intelligence.event_sequence  (build_sequence, compute_lead_times, find_recurring_patterns)
  - intelligence.dollar_flows    (estimated USD amounts)
  - llamacpp.client              (LLM narrative generation)
"""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, asdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Data Classes ──────────────────────────────────────────────────────────


@dataclass
class ForensicReport:
    """Complete forensic analysis for a single price move."""

    ticker: str
    move_date: str
    move_pct: float
    move_direction: str                  # 'up' or 'down'
    preceding_events: list[dict]         # events in the window before the move
    warning_signals: int                 # how many events pointed in the move direction
    avg_lead_time_hours: float
    key_actors: list[str]                # who was active before the move
    total_dollar_flow: float             # estimated USD flowing before the move
    narrative: str                       # LLM or rule-based explanation
    pattern_match: dict | None           # if this matches a known recurring pattern
    confidence: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ── Table Setup ───────────────────────────────────────────────────────────


def _ensure_tables(engine: Engine) -> None:
    """Create the forensic_reports table if it does not exist."""
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS forensic_reports (
                id              SERIAL PRIMARY KEY,
                ticker          TEXT NOT NULL,
                move_date       DATE NOT NULL,
                move_pct        NUMERIC,
                preceding_events JSONB,
                warning_signals INT,
                key_actors      JSONB,
                narrative       TEXT,
                pattern_match   JSONB,
                confidence      NUMERIC,
                created_at      TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(ticker, move_date)
            )
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_forensic_ticker
                ON forensic_reports (ticker, move_date DESC)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_forensic_date
                ON forensic_reports (move_date DESC)
        """))


# ── Helpers ───────────────────────────────────────────────────────────────


def _safe_float(val: Any) -> float:
    """Convert to float, defaulting to 0.0."""
    if val is None:
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def _parse_date(val: str | date | datetime) -> date:
    """Normalise a value to a date object."""
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    # ISO string
    return datetime.fromisoformat(str(val).replace("Z", "+00:00")).date()


def _direction_label(pct: float) -> str:
    """Return 'up' or 'down' from a percentage change."""
    return "up" if pct >= 0 else "down"


def _event_aligns(event_direction: str, move_direction: str) -> bool:
    """Check if an event's direction aligns with the move direction."""
    if move_direction == "up":
        return event_direction in ("bullish", "up")
    if move_direction == "down":
        return event_direction in ("bearish", "down")
    return False


def _get_price_moves(
    engine: Engine, ticker: str, days: int,
) -> list[dict[str, Any]]:
    """Fetch daily price data and compute daily returns.

    Returns list of {date, close, pct_change, direction} sorted oldest-first.
    """
    cutoff = date.today() - timedelta(days=days + 10)
    prices: list[dict[str, Any]] = []

    with engine.connect() as conn:
        # Try resolved_series first (PIT-correct)
        rows = conn.execute(text("""
            SELECT rs.obs_date, rs.value
            FROM resolved_series rs
            JOIN feature_registry fr ON fr.id = rs.feature_id
            WHERE fr.name ILIKE :fid
              AND rs.obs_date >= :c
            ORDER BY rs.obs_date
        """), {"fid": f"%{ticker}%close%", "c": cutoff}).fetchall()

        if not rows:
            # Fallback: options_daily_signals spot_price
            rows = conn.execute(text("""
                SELECT signal_date, spot_price
                FROM options_daily_signals
                WHERE ticker = :t AND signal_date >= :c AND spot_price > 0
                ORDER BY signal_date
            """), {"t": ticker.upper(), "c": cutoff}).fetchall()

        if not rows:
            # Last resort: raw_series YF close
            rows = conn.execute(text("""
                SELECT obs_date, value
                FROM raw_series
                WHERE series_id = :sid AND obs_date >= :c AND pull_status = 'SUCCESS'
                ORDER BY obs_date
            """), {"sid": f"YF:{ticker.upper()}:close", "c": cutoff}).fetchall()

    if not rows:
        return []

    prev_price: float | None = None
    for r in rows:
        dt = r[0]
        price = _safe_float(r[1])
        if price <= 0:
            continue
        pct = 0.0
        if prev_price and prev_price > 0:
            pct = (price - prev_price) / prev_price
        prev_price = price
        prices.append({
            "date": str(dt),
            "close": price,
            "pct_change": pct,
            "direction": _direction_label(pct),
        })

    return prices


# ── 1. Analyze a Single Move ─────────────────────────────────────────────


def analyze_move(
    engine: Engine,
    ticker: str,
    move_date: str,
    lookback_days: int = 14,
) -> ForensicReport | None:
    """Generate a full forensic report for a specific price move.

    Reconstructs the event timeline preceding the move, identifies who
    was active, estimates dollar flows, checks for known patterns, and
    generates an LLM narrative (rule-based fallback if offline).

    Args:
        engine: SQLAlchemy engine.
        ticker: Stock ticker symbol.
        move_date: Date of the move (YYYY-MM-DD).
        lookback_days: How many days before the move to scan.

    Returns:
        ForensicReport or None if the move date has no price data.
    """
    _ensure_tables(engine)
    ticker = ticker.upper()
    target_date = _parse_date(move_date)

    log.info(
        "Forensic analysis for {t} on {d} (lookback={lb}d)",
        t=ticker, d=target_date, lb=lookback_days,
    )

    # ── Get the move details ──────────────────────────────────────────
    price_data = _get_price_moves(engine, ticker, days=lookback_days + 30)
    if not price_data:
        log.warning("No price data for {t} — cannot analyze move", t=ticker)
        return None

    # Find the move on the target date (allow +/- 1 day for weekends)
    move_info: dict | None = None
    for p in price_data:
        p_date = _parse_date(p["date"])
        if abs((p_date - target_date).days) <= 1:
            if move_info is None or abs(p["pct_change"]) > abs(move_info["pct_change"]):
                move_info = p

    if move_info is None:
        # Use the closest date we have
        closest = min(price_data, key=lambda p: abs((_parse_date(p["date"]) - target_date).days))
        if abs((_parse_date(closest["date"]) - target_date).days) <= 5:
            move_info = closest
        else:
            log.warning("No price data near {d} for {t}", d=target_date, t=ticker)
            return None

    move_pct = move_info["pct_change"]
    move_dir = _direction_label(move_pct)

    # ── Build event sequence for the lookback window ──────────────────
    from intelligence.event_sequence import (
        build_sequence,
        compute_lead_times,
        find_recurring_patterns,
        Event,
        _get_price_series,
    )

    events = build_sequence(engine, ticker, days=lookback_days + 10)

    # Filter to events BEFORE the move date
    move_dt = datetime(target_date.year, target_date.month, target_date.day, tzinfo=timezone.utc)
    window_start = move_dt - timedelta(days=lookback_days)

    preceding: list[Event] = []
    for ev in events:
        if not ev.timestamp:
            continue
        try:
            ev_dt = datetime.fromisoformat(ev.timestamp.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            continue
        if window_start <= ev_dt < move_dt:
            preceding.append(ev)

    # Compute lead times against the price series
    price_series = _get_price_series(engine, ticker, lookback_days + 10)
    if price_series:
        preceding = compute_lead_times(preceding, price_series)

    # ── Filter events that align with move direction ──────────────────
    aligned = [ev for ev in preceding if _event_aligns(ev.direction, move_dir)]
    warning_signals = len(aligned)

    # ── Compute average lead time ─────────────────────────────────────
    lead_times = [
        ev.lead_time_to_next_move
        for ev in preceding
        if ev.lead_time_to_next_move is not None and ev.lead_time_to_next_move > 0
    ]
    avg_lead = sum(lead_times) / len(lead_times) if lead_times else 0.0

    # ── Identify key actors ───────────────────────────────────────────
    actor_counts: Counter[str] = Counter()
    for ev in aligned:
        if ev.actor:
            actor_counts[ev.actor] += 1
    # Also count actors in non-aligned events (they were active)
    for ev in preceding:
        if ev.actor and ev.actor not in actor_counts:
            actor_counts[ev.actor] += 0  # ensure they appear

    key_actors = [actor for actor, _ in actor_counts.most_common(10)]

    # ── Estimate total dollar flow ────────────────────────────────────
    total_flow = sum(_safe_float(ev.amount_usd) for ev in preceding if ev.amount_usd)

    # ── Check against recurring patterns ──────────────────────────────
    pattern_match: dict | None = None
    try:
        patterns = find_recurring_patterns(engine, min_occurrences=2)
        if patterns:
            # Build event type sequence from preceding events
            event_sig = [f"{ev.event_type}:{ev.direction}" for ev in preceding[-5:]]
            for pat in patterns:
                pat_seq = pat.get("pattern", [])
                if isinstance(pat_seq, list) and len(pat_seq) >= 2:
                    # Check if pattern appears in our preceding events
                    pat_str = "->".join(pat_seq) if isinstance(pat_seq[0], str) else ""
                    ev_str = "->".join(event_sig)
                    if pat_str and pat_str in ev_str:
                        pattern_match = pat
                        break
                elif isinstance(pat_seq, str):
                    ev_str = "->".join(event_sig)
                    if pat_seq in ev_str:
                        pattern_match = pat
                        break
    except Exception as exc:
        log.debug("Pattern matching failed: {e}", e=str(exc))

    # ── Serialize preceding events ────────────────────────────────────
    preceding_dicts = [
        {
            "timestamp": ev.timestamp,
            "event_type": ev.event_type,
            "actor": ev.actor,
            "direction": ev.direction,
            "amount_usd": ev.amount_usd,
            "description": ev.description,
            "source": ev.source,
            "confidence": ev.confidence,
            "lead_time_hours": ev.lead_time_to_next_move,
        }
        for ev in preceding
    ]

    # ── Generate narrative ────────────────────────────────────────────
    narrative = _generate_narrative(
        ticker=ticker,
        move_date=str(target_date),
        move_pct=move_pct,
        move_dir=move_dir,
        preceding=preceding,
        aligned_count=warning_signals,
        key_actors=key_actors,
        total_flow=total_flow,
        avg_lead=avg_lead,
        pattern_match=pattern_match,
    )

    # ── Confidence scoring ────────────────────────────────────────────
    # More aligned signals + more data = higher confidence
    confidence = _compute_confidence(
        total_events=len(preceding),
        aligned_events=warning_signals,
        has_pattern=pattern_match is not None,
        has_dollar_flow=total_flow > 0,
        avg_lead=avg_lead,
    )

    report = ForensicReport(
        ticker=ticker,
        move_date=str(target_date),
        move_pct=round(move_pct * 100, 4),  # store as percentage
        move_direction=move_dir,
        preceding_events=preceding_dicts,
        warning_signals=warning_signals,
        avg_lead_time_hours=round(avg_lead, 2),
        key_actors=key_actors,
        total_dollar_flow=round(total_flow, 2),
        narrative=narrative,
        pattern_match=pattern_match,
        confidence=round(confidence, 3),
    )

    # Persist
    _store_report(engine, report)

    log.info(
        "Forensic report for {t} on {d}: {dir} {pct:.2f}%, "
        "{n} events, {w} aligned, confidence={c:.2f}",
        t=ticker, d=target_date, dir=move_dir,
        pct=report.move_pct, n=len(preceding),
        w=warning_signals, c=confidence,
    )
    return report


# ── 2. Find Significant Moves ────────────────────────────────────────────


def find_significant_moves(
    engine: Engine,
    ticker: str,
    days: int = 90,
    threshold: float = 0.03,
) -> list[dict[str, Any]]:
    """Scan price data for daily moves exceeding a threshold.

    Args:
        engine: SQLAlchemy engine.
        ticker: Stock ticker symbol.
        days: Lookback window in days.
        threshold: Minimum absolute daily return (e.g. 0.03 = 3%).

    Returns:
        List of {date, pct_change, direction} for significant moves,
        sorted most recent first.
    """
    ticker = ticker.upper()
    price_data = _get_price_moves(engine, ticker, days=days)

    significant = [
        {
            "date": p["date"],
            "pct_change": round(p["pct_change"] * 100, 4),
            "direction": p["direction"],
        }
        for p in price_data
        if abs(p["pct_change"]) >= threshold
    ]

    # Most recent first
    significant.sort(key=lambda x: x["date"], reverse=True)

    log.info(
        "Found {n} significant moves (>{t:.1%}) for {tk} in {d} days",
        n=len(significant), t=threshold, tk=ticker, d=days,
    )
    return significant


# ── 3. Batch Forensics ───────────────────────────────────────────────────


def batch_forensics(
    engine: Engine,
    ticker: str,
    days: int = 90,
    threshold: float = 0.03,
) -> list[ForensicReport]:
    """Find all significant moves and generate forensic reports for each.

    Args:
        engine: SQLAlchemy engine.
        ticker: Stock ticker symbol.
        days: Lookback window for finding moves.
        threshold: Minimum absolute daily return for significance.

    Returns:
        List of ForensicReport instances, ordered most recent first.
    """
    moves = find_significant_moves(engine, ticker, days=days, threshold=threshold)
    reports: list[ForensicReport] = []

    for move in moves:
        try:
            report = analyze_move(engine, ticker, move["date"])
            if report:
                reports.append(report)
        except Exception as exc:
            log.warning(
                "Forensic analysis failed for {t} on {d}: {e}",
                t=ticker, d=move["date"], e=str(exc),
            )

    log.info(
        "Batch forensics for {t}: {n} reports from {m} significant moves",
        t=ticker, n=len(reports), m=len(moves),
    )
    return reports


# ── 4. Generate Forensic Summary ─────────────────────────────────────────


def generate_forensic_summary(engine: Engine, ticker: str, days: int = 90) -> str:
    """Produce an LLM-synthesised summary of all forensic reports for a ticker.

    Answers: how many significant moves, what preceded them, who was involved,
    and what the most reliable predictors were.

    Falls back to rule-based summary if LLM is unavailable.

    Args:
        engine: SQLAlchemy engine.
        ticker: Stock ticker symbol.
        days: Lookback window.

    Returns:
        Formatted summary string.
    """
    ticker = ticker.upper()
    reports = batch_forensics(engine, ticker, days=days)

    if not reports:
        return f"No significant moves found for {ticker} in the last {days} days."

    # Aggregate statistics
    total = len(reports)
    up_count = sum(1 for r in reports if r.move_direction == "up")
    down_count = total - up_count
    avg_signals = sum(r.warning_signals for r in reports) / total
    avg_lead = sum(r.avg_lead_time_hours for r in reports) / total
    avg_confidence = sum(r.confidence for r in reports) / total

    # Most common actors
    all_actors: Counter[str] = Counter()
    for r in reports:
        for actor in r.key_actors:
            all_actors[actor] += 1
    top_actors = all_actors.most_common(5)

    # Most common event types in preceding events
    event_type_counts: Counter[str] = Counter()
    for r in reports:
        for ev in r.preceding_events:
            event_type_counts[ev.get("event_type", "unknown")] += 1
    top_event_types = event_type_counts.most_common(5)

    # Pattern matches
    pattern_count = sum(1 for r in reports if r.pattern_match is not None)

    # Average dollar flow
    avg_flow = sum(r.total_dollar_flow for r in reports) / total

    # Try LLM synthesis
    llm_summary = _get_llm_summary(
        ticker=ticker,
        days=days,
        total=total,
        up_count=up_count,
        down_count=down_count,
        avg_signals=avg_signals,
        avg_lead=avg_lead,
        avg_confidence=avg_confidence,
        top_actors=top_actors,
        top_event_types=top_event_types,
        pattern_count=pattern_count,
        avg_flow=avg_flow,
        reports=reports,
    )
    if llm_summary:
        return llm_summary

    # Rule-based fallback
    lines = [
        f"FORENSIC SUMMARY: {ticker} ({days} days)",
        f"{'=' * 50}",
        f"",
        f"Significant moves: {total} ({up_count} up, {down_count} down)",
        f"Avg warning signals per move: {avg_signals:.1f}",
        f"Avg lead time: {avg_lead:.1f} hours",
        f"Avg confidence: {avg_confidence:.1%}",
        f"Pattern matches: {pattern_count}/{total} moves",
    ]

    if avg_flow > 0:
        lines.append(f"Avg estimated dollar flow: ${avg_flow:,.0f}")

    if top_actors:
        lines.append(f"")
        lines.append(f"Key actors (most active before moves):")
        for actor, count in top_actors:
            lines.append(f"  - {actor}: appeared before {count}/{total} moves")

    if top_event_types:
        lines.append(f"")
        lines.append(f"Most common preceding signals:")
        for evt, count in top_event_types:
            lines.append(f"  - {evt}: {count} occurrences")

    # Identify the most reliable predictor
    if top_event_types:
        best_type = top_event_types[0][0]
        lines.append(f"")
        lines.append(
            f"The most reliable predictor was {best_type} signals, "
            f"appearing {top_event_types[0][1]} times across {total} moves."
        )

    return "\n".join(lines)


# ── Storage ───────────────────────────────────────────────────────────────


def _store_report(engine: Engine, report: ForensicReport) -> None:
    """Persist a forensic report to the database (upsert on ticker+move_date)."""
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO forensic_reports
                (ticker, move_date, move_pct, preceding_events, warning_signals,
                 key_actors, narrative, pattern_match, confidence)
            VALUES
                (:ticker, :move_date, :move_pct, :preceding_events, :warning_signals,
                 :key_actors, :narrative, :pattern_match, :confidence)
            ON CONFLICT (ticker, move_date) DO UPDATE SET
                move_pct = EXCLUDED.move_pct,
                preceding_events = EXCLUDED.preceding_events,
                warning_signals = EXCLUDED.warning_signals,
                key_actors = EXCLUDED.key_actors,
                narrative = EXCLUDED.narrative,
                pattern_match = EXCLUDED.pattern_match,
                confidence = EXCLUDED.confidence,
                created_at = NOW()
        """), {
            "ticker": report.ticker,
            "move_date": report.move_date,
            "move_pct": report.move_pct,
            "preceding_events": json.dumps(report.preceding_events, default=str),
            "warning_signals": report.warning_signals,
            "key_actors": json.dumps(report.key_actors),
            "narrative": report.narrative,
            "pattern_match": json.dumps(report.pattern_match, default=str) if report.pattern_match else None,
            "confidence": report.confidence,
        })


def load_forensic_reports(
    engine: Engine,
    ticker: str,
    days: int = 90,
) -> list[dict[str, Any]]:
    """Load stored forensic reports for the API.

    Args:
        engine: SQLAlchemy engine.
        ticker: Ticker to filter on.
        days: Lookback window.

    Returns:
        List of forensic report dicts.
    """
    _ensure_tables(engine)
    cutoff = date.today() - timedelta(days=days)

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT ticker, move_date, move_pct, preceding_events,
                   warning_signals, key_actors, narrative, pattern_match,
                   confidence, created_at
            FROM forensic_reports
            WHERE ticker = :t AND move_date >= :c
            ORDER BY move_date DESC
        """), {"t": ticker.upper(), "c": cutoff}).fetchall()

    results: list[dict[str, Any]] = []
    for r in rows:
        results.append({
            "ticker": r[0],
            "move_date": str(r[1]),
            "move_pct": _safe_float(r[2]),
            "preceding_events": r[3] if isinstance(r[3], (list, dict)) else json.loads(r[3] or "[]"),
            "warning_signals": r[4],
            "key_actors": r[5] if isinstance(r[5], list) else json.loads(r[5] or "[]"),
            "narrative": r[6],
            "pattern_match": r[7] if isinstance(r[7], (dict, type(None))) else json.loads(r[7] or "null"),
            "confidence": _safe_float(r[8]),
            "created_at": str(r[9]) if r[9] else None,
        })

    return results


# ── Narrative Generation ──────────────────────────────────────────────────


def _generate_narrative(
    *,
    ticker: str,
    move_date: str,
    move_pct: float,
    move_dir: str,
    preceding: list,
    aligned_count: int,
    key_actors: list[str],
    total_flow: float,
    avg_lead: float,
    pattern_match: dict | None,
) -> str:
    """Generate a narrative explanation for the move.

    Tries LLM first, falls back to rule-based template.
    """
    # Try LLM
    llm_narrative = _get_llm_narrative(
        ticker=ticker,
        move_date=move_date,
        move_pct=move_pct,
        move_dir=move_dir,
        preceding=preceding,
        aligned_count=aligned_count,
        key_actors=key_actors,
        total_flow=total_flow,
        avg_lead=avg_lead,
        pattern_match=pattern_match,
    )
    if llm_narrative:
        return llm_narrative

    # Rule-based fallback
    pct_str = f"{abs(move_pct) * 100:.1f}%"
    lines = []
    lines.append(
        f"{ticker} moved {move_dir} {pct_str} on {move_date}."
    )

    if aligned_count > 0:
        lines.append(
            f"{aligned_count} of {len(preceding)} preceding events "
            f"pointed in the {move_dir} direction."
        )
    else:
        lines.append(
            f"None of the {len(preceding)} preceding events clearly "
            f"signaled the {move_dir} move."
        )

    if key_actors:
        lines.append(f"Key actors: {', '.join(key_actors[:5])}.")

    if total_flow > 0:
        lines.append(f"Estimated pre-move dollar flow: ${total_flow:,.0f}.")

    if avg_lead > 0:
        lines.append(f"Average signal lead time: {avg_lead:.1f} hours.")

    if pattern_match:
        pat = pattern_match.get("pattern", "unknown")
        occ = pattern_match.get("occurrences", "?")
        lines.append(f"This matches a recurring pattern ({pat}, seen {occ} times).")

    return " ".join(lines)


def _get_llm_narrative(
    *,
    ticker: str,
    move_date: str,
    move_pct: float,
    move_dir: str,
    preceding: list,
    aligned_count: int,
    key_actors: list[str],
    total_flow: float,
    avg_lead: float,
    pattern_match: dict | None,
) -> str | None:
    """Ask the LLM for a forensic narrative. Returns None if unavailable."""
    try:
        from llamacpp.client import get_client
        llm = get_client()
        if not llm.is_available:
            return None
    except Exception:
        return None

    # Build event summary (truncated for context window)
    event_lines: list[str] = []
    for ev in preceding[-20:]:  # last 20 events
        ts = getattr(ev, "timestamp", ev.get("timestamp", "")) if isinstance(ev, dict) else ev.timestamp
        etype = getattr(ev, "event_type", "") if not isinstance(ev, dict) else ev.get("event_type", "")
        direction = getattr(ev, "direction", "") if not isinstance(ev, dict) else ev.get("direction", "")
        desc = getattr(ev, "description", "") if not isinstance(ev, dict) else ev.get("description", "")
        actor = getattr(ev, "actor", None) if not isinstance(ev, dict) else ev.get("actor")
        amount = getattr(ev, "amount_usd", None) if not isinstance(ev, dict) else ev.get("amount_usd")

        line = f"  {ts}: [{etype}] {direction}"
        if actor:
            line += f" by {actor}"
        if amount:
            line += f" (${float(amount):,.0f})"
        if desc:
            line += f" — {str(desc)[:100]}"
        event_lines.append(line)

    events_text = "\n".join(event_lines) if event_lines else "  (no events)"

    pattern_text = ""
    if pattern_match:
        pattern_text = f"\nKNOWN PATTERN MATCH: {json.dumps(pattern_match, default=str)[:300]}"

    prompt = (
        f"You are a forensic market analyst. Explain what preceded this price move.\n\n"
        f"MOVE: {ticker} moved {move_dir} {abs(move_pct) * 100:.2f}% on {move_date}\n"
        f"ALIGNED SIGNALS: {aligned_count} of {len(preceding)} events pointed {move_dir}\n"
        f"KEY ACTORS: {', '.join(key_actors[:5]) or 'none identified'}\n"
        f"DOLLAR FLOW: ${total_flow:,.0f}\n"
        f"AVG LEAD TIME: {avg_lead:.1f} hours\n"
        f"{pattern_text}\n\n"
        f"PRECEDING EVENTS (chronological):\n{events_text}\n\n"
        f"Write a 2-4 sentence forensic narrative connecting the dots. "
        f"Be specific about which actors/signals preceded the move and why. "
        f"If the move was NOT well-signaled, say so."
    )

    try:
        response = llm.generate(
            prompt=prompt,
            system=(
                "You are a forensic market analyst for GRID intelligence. "
                "Be concise, data-driven, and specific. No hedging or disclaimers."
            ),
            temperature=0.3,
            num_predict=500,
        )
        if response and len(response.strip()) > 20:
            return response.strip()
        return None
    except Exception as exc:
        log.debug("LLM forensic narrative failed: {e}", e=str(exc))
        return None


def _get_llm_summary(
    *,
    ticker: str,
    days: int,
    total: int,
    up_count: int,
    down_count: int,
    avg_signals: float,
    avg_lead: float,
    avg_confidence: float,
    top_actors: list[tuple[str, int]],
    top_event_types: list[tuple[str, int]],
    pattern_count: int,
    avg_flow: float,
    reports: list[ForensicReport],
) -> str | None:
    """Ask the LLM for a forensic summary across all reports. Returns None if unavailable."""
    try:
        from llamacpp.client import get_client
        llm = get_client()
        if not llm.is_available:
            return None
    except Exception:
        return None

    actors_str = ", ".join(f"{a} ({c}x)" for a, c in top_actors[:5]) or "none"
    events_str = ", ".join(f"{e} ({c}x)" for e, c in top_event_types[:5]) or "none"

    # Sample narratives from reports
    sample_narratives = "\n".join(
        f"  - {r.move_date}: {r.move_direction} {r.move_pct:.1f}% — {r.narrative[:150]}"
        for r in reports[:5]
    )

    prompt = (
        f"Synthesise a forensic intelligence summary for {ticker} over {days} days.\n\n"
        f"STATS:\n"
        f"  Significant moves: {total} ({up_count} up, {down_count} down)\n"
        f"  Avg aligned signals per move: {avg_signals:.1f}\n"
        f"  Avg lead time: {avg_lead:.1f} hours\n"
        f"  Pattern matches: {pattern_count}/{total}\n"
        f"  Avg dollar flow: ${avg_flow:,.0f}\n"
        f"  Avg confidence: {avg_confidence:.1%}\n\n"
        f"TOP ACTORS: {actors_str}\n"
        f"TOP SIGNAL TYPES: {events_str}\n\n"
        f"SAMPLE MOVES:\n{sample_narratives}\n\n"
        f"Write a 3-5 sentence executive summary. Highlight the most reliable "
        f"predictor, the key actors, and any systematic pattern. Be specific."
    )

    try:
        response = llm.generate(
            prompt=prompt,
            system=(
                "You are the GRID forensic intelligence system. "
                "Produce actionable, data-driven summaries. No disclaimers."
            ),
            temperature=0.3,
            num_predict=600,
        )
        if response and len(response.strip()) > 30:
            return response.strip()
        return None
    except Exception as exc:
        log.debug("LLM forensic summary failed: {e}", e=str(exc))
        return None


# ── Confidence Scoring ────────────────────────────────────────────────────


def _compute_confidence(
    *,
    total_events: int,
    aligned_events: int,
    has_pattern: bool,
    has_dollar_flow: bool,
    avg_lead: float,
) -> float:
    """Compute a 0-1 confidence score for a forensic report.

    Factors:
      - More data (events) = higher base confidence
      - Higher ratio of aligned signals = stronger
      - Pattern match = bonus
      - Dollar flow data = bonus
      - Reasonable lead time = bonus
    """
    if total_events == 0:
        return 0.1

    # Base: data sufficiency (0.1 to 0.4)
    base = min(0.4, 0.1 + (total_events / 50.0) * 0.3)

    # Alignment ratio (0.0 to 0.3)
    alignment = (aligned_events / total_events) * 0.3

    # Bonuses
    pattern_bonus = 0.1 if has_pattern else 0.0
    flow_bonus = 0.05 if has_dollar_flow else 0.0
    lead_bonus = 0.05 if 1.0 < avg_lead < 336.0 else 0.0  # 1h to 14 days

    confidence = base + alignment + pattern_bonus + flow_bonus + lead_bonus
    return min(1.0, max(0.0, confidence))
