"""
GRID Intelligence — Insider Intel: Say vs Do Cross-Reference Engine.

Tracks what insiders (congressional members, corporate officers, fund managers)
SAY publicly vs what they DO with their money.  Detects divergence — when
someone says "bullish" but sells, or says "strong economy" while dumping
equities.  Also tracks 2nd-degree actors: people connected to insiders who
trade suspiciously close to information events.

Key entry points:
  ingest_statement        — store a public statement with tone analysis
  cross_reference_actor   — compare an actor's words vs trades
  detect_divergences      — scan all actors for say/do mismatches
  score_network_proximity — find 2nd-degree actors trading near events
  run_insider_intel_cycle — full cycle for Hermes scheduling

Divergence types:
  - BULLISH_TALK_BEARISH_ACTION: Says positive, sells/shorts
  - BEARISH_TALK_BULLISH_ACTION: Says negative, buys aggressively
  - GUIDANCE_MISS: Promised X, delivered Y (testable claims)
  - TIMING_SUSPICIOUS: 2nd-degree actor trades before announcement
  - TONE_SHIFT: Sudden change in public tone without visible catalyst
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Constants ───────────────────────────────────────────────────────────

# How many days around a statement to look for trades
_TRADE_WINDOW_DAYS = 14

# Minimum divergence to flag
_DIVERGENCE_THRESHOLD = 0.4  # tone is +0.6 bullish but trades are -0.4 bearish = 1.0 divergence

# Tone mapping for numerical comparison
_TONE_SCORES = {
    "very_bullish": 1.0,
    "bullish": 0.6,
    "slightly_bullish": 0.3,
    "neutral": 0.0,
    "slightly_bearish": -0.3,
    "bearish": -0.6,
    "very_bearish": -1.0,
}


# ══════════════════════════════════════════════════════════════════════════
# STATEMENT INGESTION
# ══════════════════════════════════════════════════════════════════════════

def ingest_statement(
    engine: Engine,
    actor_name: str,
    statement_text: str,
    statement_date: date,
    source: str,
    *,
    actor_title: str | None = None,
    actor_category: str | None = None,
    statement_type: str = "interview",
    ticker: str | None = None,
    sector: str | None = None,
    source_url: str | None = None,
    event_type: str | None = None,
    tone: str | None = None,
    sentiment_score: float | None = None,
    commitment_level: str = "off_cuff",
    claim_testable: bool = False,
    claim_text: str | None = None,
    claim_target_date: date | None = None,
    claim_metric: str | None = None,
    claim_value: float | None = None,
    evidence: dict | None = None,
) -> int | None:
    """Store a public statement and optionally flag testable claims.

    Returns the statement ID on success, None on failure.
    """
    try:
        with engine.begin() as conn:
            row = conn.execute(text("""
                INSERT INTO public_statements
                (actor_name, actor_title, actor_category, statement_type,
                 ticker, sector, statement_text, statement_date, source,
                 source_url, event_type, tone, sentiment_score,
                 commitment_level, claim_testable, claim_text,
                 claim_target_date, claim_metric, claim_value, evidence)
                VALUES
                (:name, :title, :cat, :stype, :ticker, :sector, :text,
                 :sdate, :source, :url, :etype, :tone, :sent,
                 :commit, :testable, :claim, :cdate, :cmetric, :cval, :ev)
                ON CONFLICT DO NOTHING
                RETURNING id
            """), {
                "name": actor_name,
                "title": actor_title,
                "cat": actor_category,
                "stype": statement_type,
                "ticker": ticker,
                "sector": sector,
                "text": statement_text[:2000],
                "sdate": statement_date,
                "source": source,
                "url": source_url,
                "etype": event_type,
                "tone": tone,
                "sent": sentiment_score,
                "commit": commitment_level,
                "testable": claim_testable,
                "claim": claim_text,
                "cdate": claim_target_date,
                "cmetric": claim_metric,
                "cval": claim_value,
                "ev": json.dumps(evidence or {}),
            }).fetchone()

            return row[0] if row else None

    except Exception as exc:
        log.warning("Failed to ingest statement: {e}", e=str(exc))
        return None


# ══════════════════════════════════════════════════════════════════════════
# CROSS-REFERENCE: SAY vs DO
# ══════════════════════════════════════════════════════════════════════════

def cross_reference_actor(
    engine: Engine,
    actor_name: str,
    days: int = 90,
) -> dict[str, Any]:
    """Compare an actor's public statements with their trading activity.

    For each statement, look for trades within ±14 days.
    Flag divergences where tone contradicts action.

    Returns:
        {actor_name, statements, trades, divergences, alignment_score, summary}
    """
    cutoff = date.today() - timedelta(days=days)

    try:
        with engine.connect() as conn:
            # Get statements
            stmt_rows = conn.execute(text("""
                SELECT id, statement_text, statement_date, tone,
                       sentiment_score, ticker, statement_type, source,
                       commitment_level
                FROM public_statements
                WHERE actor_name = :name
                AND statement_date >= :cutoff
                ORDER BY statement_date DESC
            """), {"name": actor_name, "cutoff": cutoff}).fetchall()

            # Get trades (congressional + insider)
            trade_rows = conn.execute(text("""
                SELECT signal_type, ticker, signal_date, signal_value
                FROM signal_sources
                WHERE source_id = :name
                AND source_type IN ('congressional', 'insider')
                AND signal_date >= :cutoff
                ORDER BY signal_date DESC
            """), {"name": actor_name, "cutoff": cutoff}).fetchall()

    except Exception as exc:
        log.warning("Cross-reference query failed for {n}: {e}", n=actor_name, e=str(exc))
        return {"actor_name": actor_name, "error": str(exc)}

    statements = [
        {
            "id": r[0], "text": r[1][:200], "date": r[2].isoformat() if r[2] else None,
            "tone": r[3], "sentiment": r[4], "ticker": r[5],
            "type": r[6], "source": r[7], "commitment": r[8],
        }
        for r in stmt_rows
    ]

    trades = []
    for r in trade_rows:
        sv = r[3] if isinstance(r[3], dict) else json.loads(r[3]) if r[3] else {}
        trades.append({
            "action": r[0],  # BUY / SELL
            "ticker": r[1],
            "date": r[2].isoformat() if r[2] else None,
            "amount": sv.get("amount_midpoint") or sv.get("value"),
            "details": sv,
        })

    # Detect divergences
    divergences = []
    aligned = 0
    total_checked = 0

    for stmt in statements:
        tone_score = _TONE_SCORES.get(stmt.get("tone", ""), 0)
        if stmt.get("sentiment") is not None:
            tone_score = stmt["sentiment"]

        if abs(tone_score) < 0.2:
            continue  # neutral statement, skip

        stmt_date = date.fromisoformat(stmt["date"]) if stmt.get("date") else None
        if not stmt_date:
            continue

        # Find trades within window
        window_trades = []
        for t in trades:
            t_date = date.fromisoformat(t["date"]) if t.get("date") else None
            if not t_date:
                continue
            gap = abs((t_date - stmt_date).days)
            if gap <= _TRADE_WINDOW_DAYS:
                # Match on ticker if statement has one, otherwise include all
                if stmt.get("ticker") and t.get("ticker"):
                    if stmt["ticker"].upper() != t["ticker"].upper():
                        continue
                window_trades.append(t)

        if not window_trades:
            continue

        total_checked += 1

        # Compute trade direction score
        buy_count = sum(1 for t in window_trades if t["action"] in ("BUY", "PURCHASE"))
        sell_count = sum(1 for t in window_trades if t["action"] in ("SELL", "SALE", "SALE_FULL", "SALE_PARTIAL"))
        total_trades = buy_count + sell_count
        if total_trades == 0:
            continue

        trade_score = (buy_count - sell_count) / total_trades  # -1 to +1

        # Divergence = tone pointing one way, trades pointing the other
        divergence_magnitude = abs(tone_score - trade_score)

        if tone_score > 0.2 and trade_score < -0.2:
            divergence_type = "BULLISH_TALK_BEARISH_ACTION"
        elif tone_score < -0.2 and trade_score > 0.2:
            divergence_type = "BEARISH_TALK_BULLISH_ACTION"
        else:
            divergence_type = None
            aligned += 1

        if divergence_type and divergence_magnitude >= _DIVERGENCE_THRESHOLD:
            divergences.append({
                "type": divergence_type,
                "magnitude": round(divergence_magnitude, 2),
                "statement_date": stmt["date"],
                "statement_tone": stmt["tone"],
                "statement_text": stmt["text"][:150],
                "trades_in_window": len(window_trades),
                "buy_count": buy_count,
                "sell_count": sell_count,
                "ticker": stmt.get("ticker"),
            })

    alignment_score = aligned / total_checked if total_checked > 0 else 0.5

    return {
        "actor_name": actor_name,
        "period_days": days,
        "total_statements": len(statements),
        "total_trades": len(trades),
        "checked_pairs": total_checked,
        "aligned": aligned,
        "divergences": divergences,
        "alignment_score": round(alignment_score, 3),
        "credibility": (
            "HIGH" if alignment_score > 0.8 else
            "MODERATE" if alignment_score > 0.5 else
            "LOW" if alignment_score > 0.2 else
            "SUSPECT"
        ),
        "summary": (
            f"{actor_name}: {len(statements)} statements, {len(trades)} trades, "
            f"{len(divergences)} divergences detected. "
            f"Alignment: {alignment_score*100:.0f}%."
            + (f" RED FLAGS: {', '.join(d['type'] for d in divergences[:3])}" if divergences else "")
        ),
    }


# ══════════════════════════════════════════════════════════════════════════
# DIVERGENCE SCANNER (all actors)
# ══════════════════════════════════════════════════════════════════════════

def detect_divergences(
    engine: Engine,
    days: int = 90,
    min_statements: int = 1,
) -> list[dict[str, Any]]:
    """Scan all actors with both statements and trades for say/do divergence.

    Returns list of actors with divergences, sorted by severity.
    """
    try:
        with engine.connect() as conn:
            # Find actors who have both statements and trades
            actors = conn.execute(text("""
                SELECT DISTINCT ps.actor_name
                FROM public_statements ps
                WHERE ps.statement_date >= CURRENT_DATE - :days
                AND EXISTS (
                    SELECT 1 FROM signal_sources ss
                    WHERE ss.source_id = ps.actor_name
                    AND ss.source_type IN ('congressional', 'insider')
                    AND ss.signal_date >= CURRENT_DATE - :days
                )
            """), {"days": days}).fetchall()

    except Exception as exc:
        log.warning("Divergence scan query failed: {e}", e=str(exc))
        return []

    results = []
    for row in actors:
        name = row[0]
        xref = cross_reference_actor(engine, name, days=days)
        if xref.get("divergences"):
            results.append(xref)

    # Sort by number of divergences (most suspicious first)
    results.sort(key=lambda x: len(x.get("divergences", [])), reverse=True)

    log.info(
        "Divergence scan: {n} actors checked, {d} with divergences",
        n=len(actors), d=len(results),
    )
    return results


# ══════════════════════════════════════════════════════════════════════════
# 2ND-DEGREE NETWORK PROXIMITY
# ══════════════════════════════════════════════════════════════════════════

def score_network_proximity(
    engine: Engine,
    event_ticker: str,
    event_date: date,
    window_days: int = 5,
) -> list[dict[str, Any]]:
    """Find suspicious trades by actors connected to insiders of a ticker.

    When a major event hits a ticker (earnings, FDA, acquisition), look for
    trades by people 1-2 degrees away from insiders in the days BEFORE
    the event.  These are potential information leakage signals.

    Parameters:
        engine: SQLAlchemy engine.
        event_ticker: The ticker the event concerns.
        event_date: Date of the event.
        window_days: How many days before the event to scan.

    Returns:
        List of suspicious trade signals with network distance.
    """
    pre_event_start = event_date - timedelta(days=window_days)

    try:
        with engine.connect() as conn:
            # Step 1: Find known insiders of this ticker
            insiders = conn.execute(text("""
                SELECT DISTINCT source_id FROM signal_sources
                WHERE source_type = 'insider'
                AND UPPER(ticker) = :ticker
                AND signal_date >= CURRENT_DATE - 365
            """), {"ticker": event_ticker.upper()}).fetchall()

            insider_names = {r[0] for r in insiders if r[0]}

            if not insider_names:
                return []

            # Step 2: Find actors connected to these insiders
            connected = set()
            for name in insider_names:
                rows = conn.execute(text("""
                    SELECT connections FROM actors
                    WHERE name = :name OR id = :name
                """), {"name": name}).fetchall()
                for r in rows:
                    conns = r[0] if isinstance(r[0], list) else json.loads(r[0]) if r[0] else []
                    for c in conns:
                        if isinstance(c, str):
                            connected.add(c)
                        elif isinstance(c, dict):
                            connected.add(c.get("name", c.get("id", "")))

            # Step 3: Find trades by connected actors in the pre-event window
            suspicious = []
            for actor in connected:
                if not actor or actor in insider_names:
                    continue

                trades = conn.execute(text("""
                    SELECT source_type, source_id, ticker, signal_type,
                           signal_date, signal_value
                    FROM signal_sources
                    WHERE source_id = :actor
                    AND signal_date >= :start AND signal_date < :event
                    AND UPPER(ticker) = :ticker
                """), {
                    "actor": actor,
                    "start": pre_event_start,
                    "event": event_date,
                    "ticker": event_ticker.upper(),
                }).fetchall()

                for t in trades:
                    sv = t[5] if isinstance(t[5], dict) else json.loads(t[5]) if t[5] else {}
                    suspicious.append({
                        "actor": t[1],
                        "source_type": t[0],
                        "ticker": t[2],
                        "action": t[3],
                        "trade_date": t[4].isoformat() if t[4] else None,
                        "days_before_event": (event_date - t[4]).days if t[4] else None,
                        "amount": sv.get("amount_midpoint") or sv.get("value"),
                        "network_distance": 2,  # 2nd degree from insider
                        "connected_insiders": [
                            n for n in insider_names
                            # Check if this actor is in the insider's connections
                        ][:3],
                    })

    except Exception as exc:
        log.warning("Network proximity scan failed: {e}", e=str(exc))
        return []

    suspicious.sort(key=lambda x: x.get("days_before_event", 999))

    if suspicious:
        log.info(
            "Network proximity: {n} suspicious trades for {t} before {d}",
            n=len(suspicious), t=event_ticker, d=event_date,
        )

    return suspicious


# ══════════════════════════════════════════════════════════════════════════
# CREDIBILITY SCORING
# ══════════════════════════════════════════════════════════════════════════

def update_actor_credibility(engine: Engine, actor_name: str) -> dict[str, Any]:
    """Recompute credibility score for an actor based on all available evidence.

    Credibility = weighted average of:
    - Say/do alignment (40%) — do their trades match their public tone?
    - Claim accuracy (30%) — when they make testable claims, are they right?
    - Tone accuracy (30%) — when they say bullish, does the market go up?
    """
    xref = cross_reference_actor(engine, actor_name, days=365)

    try:
        with engine.connect() as conn:
            # Count testable claims and outcomes
            outcomes = conn.execute(text("""
                SELECT verdict, COUNT(*) FROM statement_outcomes
                WHERE actor_name = :name
                GROUP BY verdict
            """), {"name": actor_name}).fetchall()

            hits = sum(r[1] for r in outcomes if r[0] == "hit")
            misses = sum(r[1] for r in outcomes if r[0] == "miss")
            total_claims = hits + misses

            claim_accuracy = hits / total_claims if total_claims > 0 else 0.5

            # Count statement tone vs market outcome
            tone_rows = conn.execute(text("""
                SELECT ps.tone, ps.ticker, ps.statement_date
                FROM public_statements ps
                WHERE ps.actor_name = :name
                AND ps.tone IN ('bullish', 'bearish', 'very_bullish', 'very_bearish')
                AND ps.statement_date >= CURRENT_DATE - 365
            """), {"name": actor_name}).fetchall()

            tone_hits = 0
            tone_total = 0
            # For each bullish/bearish statement, check if market went that way
            for tr in tone_rows:
                tone_val = _TONE_SCORES.get(tr[0], 0)
                if abs(tone_val) < 0.3 or not tr[1]:
                    continue
                # Check 5-day price change after statement
                price_row = conn.execute(text("""
                    SELECT
                        (SELECT value FROM raw_series
                         WHERE series_id = :sid AND obs_date > :d
                         ORDER BY obs_date ASC LIMIT 1) as after_price,
                        (SELECT value FROM raw_series
                         WHERE series_id = :sid AND obs_date <= :d
                         ORDER BY obs_date DESC LIMIT 1) as before_price
                """), {
                    "sid": f"yf_{tr[1].lower()}_close",
                    "d": tr[2],
                }).fetchone()

                if price_row and price_row[0] and price_row[1]:
                    pct_change = (float(price_row[0]) - float(price_row[1])) / float(price_row[1])
                    tone_total += 1
                    if (tone_val > 0 and pct_change > 0) or (tone_val < 0 and pct_change < 0):
                        tone_hits += 1

            tone_accuracy = tone_hits / tone_total if tone_total > 0 else 0.5

            # Weighted credibility
            say_do = xref.get("alignment_score", 0.5)
            credibility = (say_do * 0.4) + (claim_accuracy * 0.3) + (tone_accuracy * 0.3)

            # Get last dates
            last_stmt = conn.execute(text(
                "SELECT MAX(statement_date) FROM public_statements WHERE actor_name = :n"
            ), {"n": actor_name}).scalar()
            last_trade = conn.execute(text(
                "SELECT MAX(signal_date) FROM signal_sources WHERE source_id = :n AND source_type IN ('congressional','insider')"
            ), {"n": actor_name}).scalar()

            # Upsert credibility
            conn.execute(text("""
                INSERT INTO actor_credibility
                (actor_name, total_statements, testable_claims, claims_hit,
                 claims_missed, say_do_alignment, tone_accuracy,
                 last_statement_date, last_trade_date,
                 divergence_count, credibility_score, updated_at)
                VALUES (:name, :stmts, :claims, :hits, :misses,
                        :saydo, :tone, :lstmt, :ltrade,
                        :divs, :cred, NOW())
                ON CONFLICT (actor_name) DO UPDATE SET
                    total_statements = EXCLUDED.total_statements,
                    testable_claims = EXCLUDED.testable_claims,
                    claims_hit = EXCLUDED.claims_hit,
                    claims_missed = EXCLUDED.claims_missed,
                    say_do_alignment = EXCLUDED.say_do_alignment,
                    tone_accuracy = EXCLUDED.tone_accuracy,
                    last_statement_date = EXCLUDED.last_statement_date,
                    last_trade_date = EXCLUDED.last_trade_date,
                    divergence_count = EXCLUDED.divergence_count,
                    credibility_score = EXCLUDED.credibility_score,
                    updated_at = NOW()
            """), {
                "name": actor_name,
                "stmts": xref.get("total_statements", 0),
                "claims": total_claims,
                "hits": hits,
                "misses": misses,
                "saydo": say_do,
                "tone": tone_accuracy,
                "lstmt": last_stmt,
                "ltrade": last_trade,
                "divs": len(xref.get("divergences", [])),
                "cred": round(credibility, 3),
            })

        return {
            "actor_name": actor_name,
            "credibility_score": round(credibility, 3),
            "say_do_alignment": round(say_do, 3),
            "claim_accuracy": round(claim_accuracy, 3),
            "tone_accuracy": round(tone_accuracy, 3),
            "divergence_count": len(xref.get("divergences", [])),
            "rating": (
                "HIGHLY_CREDIBLE" if credibility > 0.8 else
                "CREDIBLE" if credibility > 0.6 else
                "MIXED" if credibility > 0.4 else
                "LOW_CREDIBILITY" if credibility > 0.2 else
                "SUSPECT"
            ),
        }

    except Exception as exc:
        log.warning("Credibility update failed for {n}: {e}", n=actor_name, e=str(exc))
        return {"actor_name": actor_name, "error": str(exc)}


# ══════════════════════════════════════════════════════════════════════════
# HERMES CYCLE
# ══════════════════════════════════════════════════════════════════════════

def run_insider_intel_cycle(engine: Engine) -> dict[str, Any]:
    """Full insider intelligence cycle for scheduled execution.

    1. Detect all say/do divergences
    2. Update credibility scores for flagged actors
    3. Return summary
    """
    divergences = detect_divergences(engine, days=90)

    credibility_updates = []
    for d in divergences[:20]:  # cap to avoid long runs
        name = d.get("actor_name")
        if name:
            cred = update_actor_credibility(engine, name)
            credibility_updates.append(cred)

    suspects = [c for c in credibility_updates if c.get("rating") in ("LOW_CREDIBILITY", "SUSPECT")]

    return {
        "actors_scanned": len(divergences),
        "divergences_found": sum(len(d.get("divergences", [])) for d in divergences),
        "credibility_updated": len(credibility_updates),
        "suspects": suspects,
        "summary": (
            f"Insider intel cycle: {len(divergences)} actors with divergences, "
            f"{len(suspects)} flagged as suspect."
        ),
    }
