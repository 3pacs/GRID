"""
GRID Intelligence — Pocket-Lining Detection.

Detects self-dealing, conflicts of interest, and suspicious trading patterns:
  1. Politician trades in sector their committee oversees
  2. Fund manager personal trades diverge from fund trades
  3. Insider sells right before bad news
  4. Coordinated political buying (potential shared non-public information)

Entry point:
    assess_pocket_lining(engine)  — returns list of flag dicts
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


def assess_pocket_lining(engine: Engine) -> list[dict]:
    """Detect self-dealing, conflicts of interest, and suspicious patterns.

    Detections:
      1. Politician trades in sector their committee oversees
      2. Fund manager personal trades diverge from fund trades
      3. Insider sells right before bad news
      4. Lobbying spend correlates with favorable regulation

    Parameters:
        engine: SQLAlchemy engine.

    Returns:
        List of dicts, each describing a suspicious pattern with
        who, what, who_benefits, confidence, implication.
    """
    # Import here to avoid circular import — actor_network imports from us,
    # and we need its internal state (_KNOWN_ACTORS, lookup maps, _ensure_tables).
    from intelligence.actor_network import (
        _KNOWN_ACTORS,
        _SECTOR_COMMITTEE_MAP,
        _TICKER_SECTOR,
        _ensure_tables,
    )

    _ensure_tables(engine)
    flags: list[dict] = []
    cutoff = date.today() - timedelta(days=90)

    # ── Detection 1: Politicians trading in their committee's jurisdiction ──
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT source_id, ticker, signal_type, signal_date,
                       signal_value
                FROM signal_sources
                WHERE source_type = 'congressional'
                  AND signal_date >= :cutoff
                ORDER BY signal_date DESC
            """), {"cutoff": cutoff}).fetchall()

            for r in rows:
                member = str(r[0])
                ticker = str(r[1])
                direction = str(r[2])
                sig_date = str(r[3])

                sector_etf = _TICKER_SECTOR.get(ticker)
                if not sector_etf:
                    continue

                committee_keywords = _SECTOR_COMMITTEE_MAP.get(sector_etf, set())
                if not committee_keywords:
                    continue

                member_actor = None
                for aid, actor in _KNOWN_ACTORS.items():
                    if (
                        actor["name"].lower() in member.lower()
                        or member.lower() in actor["name"].lower()
                    ):
                        member_actor = actor
                        break

                if member_actor:
                    title_lower = member_actor.get("title", "").lower()
                    matching_committees = [
                        kw for kw in committee_keywords if kw in title_lower
                    ]
                    if matching_committees:
                        flags.append({
                            "detection": "committee_jurisdiction_trade",
                            "who": member,
                            "what": f"{direction} {ticker} on {sig_date}",
                            "who_benefits": member,
                            "overlap": (
                                f"Committees: {', '.join(matching_committees)}; "
                                f"Sector: {sector_etf}"
                            ),
                            "confidence": "likely",
                            "implication": (
                                f"{member} traded {ticker} ({sector_etf} sector) while "
                                f"serving on committee with jurisdiction over that sector"
                            ),
                            "severity": "high",
                        })
    except Exception as exc:
        log.debug("Committee jurisdiction check failed: {e}", e=str(exc))

    # ── Detection 2: Fund manager personal trades diverge from fund ────────
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                WITH insider_trades AS (
                    SELECT source_id, ticker, signal_type, signal_date
                    FROM signal_sources
                    WHERE source_type = 'insider'
                      AND signal_date >= :cutoff
                ),
                fund_trades AS (
                    SELECT source_id, ticker, signal_type, signal_date
                    FROM signal_sources
                    WHERE source_type = 'institutional'
                      AND signal_date >= :cutoff
                )
                SELECT i.source_id AS insider_name,
                       i.ticker,
                       i.direction AS insider_direction,
                       f.direction AS fund_direction,
                       i.signal_date
                FROM insider_trades i
                JOIN fund_trades f ON i.ticker = f.ticker
                    AND ABS(i.signal_date - f.signal_date) <= 30
                WHERE i.direction != f.direction
                LIMIT 100
            """), {"cutoff": cutoff}).fetchall()

            for r in rows:
                flags.append({
                    "detection": "fund_manager_divergence",
                    "who": str(r[0]),
                    "what": (
                        f"Personal: {r[2]} {r[1]}; "
                        f"Fund: {r[3]} {r[1]} on {r[4]}"
                    ),
                    "who_benefits": str(r[0]),
                    "confidence": "likely",
                    "implication": (
                        f"Insider {r[0]} is personally trading {r[2]} {r[1]} "
                        f"while their fund is doing the opposite ({r[3]})"
                    ),
                    "severity": "high",
                })
    except Exception as exc:
        log.debug("Fund manager divergence check failed: {e}", e=str(exc))

    # ── Detection 3: Insider sells before bad news ─────────────────────────
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT ss.source_id, ss.ticker, ss.signal_date, ss.trust_score
                FROM signal_sources ss
                WHERE ss.source_type = 'insider'
                  AND ss.signal_type = 'SELL'
                  AND ss.signal_date >= :cutoff
                ORDER BY ss.signal_date DESC
                LIMIT 200
            """), {"cutoff": cutoff}).fetchall()

            for r in rows:
                insider_name = str(r[0])
                ticker = str(r[1])
                sell_date = r[2]
                trust = float(r[3]) if r[3] else 0.5

                if sell_date is None:
                    continue

                check_date = (
                    sell_date + timedelta(days=14)
                    if isinstance(sell_date, date)
                    else None
                )
                if check_date and check_date <= date.today():
                    price_row = conn.execute(text("""
                        SELECT value FROM raw_series
                        WHERE series_id = :sid
                          AND obs_date BETWEEN :d1 AND :d2
                          AND pull_status = 'SUCCESS'
                        ORDER BY obs_date DESC LIMIT 1
                    """), {
                        "sid": f"YF:{ticker}:close",
                        "d1": sell_date,
                        "d2": check_date,
                    }).fetchone()

                    price_before_row = conn.execute(text("""
                        SELECT value FROM raw_series
                        WHERE series_id = :sid
                          AND obs_date <= :d
                          AND pull_status = 'SUCCESS'
                        ORDER BY obs_date DESC LIMIT 1
                    """), {
                        "sid": f"YF:{ticker}:close",
                        "d": sell_date,
                    }).fetchone()

                    if price_row and price_before_row:
                        after = float(price_row[0])
                        before = float(price_before_row[0])
                        if before > 0:
                            pct_change = (after - before) / before
                            if pct_change < -0.05:
                                flags.append({
                                    "detection": "insider_sell_before_drop",
                                    "who": insider_name,
                                    "what": (
                                        f"Sold {ticker} on {sell_date}, "
                                        f"price dropped {pct_change*100:.1f}% within 14 days"
                                    ),
                                    "who_benefits": insider_name,
                                    "confidence": (
                                        "confirmed" if pct_change < -0.10 else "likely"
                                    ),
                                    "implication": (
                                        f"{insider_name} sold {ticker} before a "
                                        f"{abs(pct_change)*100:.1f}% decline. "
                                        f"Trust score: {trust:.2f}"
                                    ),
                                    "severity": (
                                        "critical" if pct_change < -0.10 else "high"
                                    ),
                                })
    except Exception as exc:
        log.debug("Insider pre-drop check failed: {e}", e=str(exc))

    # ── Detection 4: Coordinated political buying ──────────────────────────
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT source_id, ticker, signal_type, signal_date
                FROM signal_sources
                WHERE source_type IN ('congressional', 'insider')
                  AND signal_date >= :cutoff
                  AND direction = 'BUY'
                ORDER BY signal_date DESC
                LIMIT 100
            """), {"cutoff": cutoff}).fetchall()

            ticker_buyers: dict[str, list[str]] = defaultdict(list)
            for r in rows:
                ticker_buyers[str(r[1])].append(str(r[0]))

            for ticker, buyers in ticker_buyers.items():
                if len(buyers) >= 3:
                    unique_buyers = list(set(buyers))
                    if len(unique_buyers) >= 3:
                        flags.append({
                            "detection": "coordinated_political_buying",
                            "who": ", ".join(unique_buyers[:5]),
                            "what": (
                                f"{len(unique_buyers)} unique actors buying "
                                f"{ticker} within 90 days"
                            ),
                            "who_benefits": f"All buyers of {ticker}",
                            "confidence": "rumored",
                            "implication": (
                                f"Coordinated buying in {ticker} by {len(unique_buyers)} actors. "
                                f"May indicate shared non-public information."
                            ),
                            "severity": "moderate",
                        })
    except Exception as exc:
        log.debug("Lobbying correlation check failed: {e}", e=str(exc))

    # Sort by severity
    severity_order = {"critical": 0, "high": 1, "moderate": 2, "low": 3}
    flags.sort(key=lambda x: severity_order.get(x.get("severity", "low"), 3))

    log.info("Pocket-lining assessment: {n} flags raised", n=len(flags))
    return flags
