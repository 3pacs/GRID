"""
GRID Intelligence — Wealth Tracking & Migration.

Tracks where money is moving by aggregating signals from:
  - 13F filings (institutional position changes)
  - Congressional disclosures (politician buys/sells)
  - Form 4 insider filings (accumulation/dumping)
  - Dark pool signals (anonymous large flows)

Entry points:
    track_wealth_migration(engine, days)  — list of WealthFlow objects
    persist_wealth_flows(engine, flows)   — persist to DB, returns count
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# Circular import note: actor_network imports track_wealth_migration from this
# module, so we cannot import WealthFlow / _ensure_tables from actor_network at
# module level.  All actor_network symbols are imported lazily inside each
# function body below.


def _parse_signal_value(val: Any) -> dict:
    """Parse signal_value which may be a JSON string, dict, or None."""
    if val is None:
        return {}
    if isinstance(val, dict):
        return val
    if isinstance(val, str):
        try:
            parsed = json.loads(val)
            return parsed if isinstance(parsed, dict) else {}
        except (json.JSONDecodeError, TypeError):
            return {}
    return {}


def track_wealth_migration(
    engine: Engine,
    days: int = 90,
) -> list:
    """Track where money is moving over the last N days.

    Aggregates data from:
      - 13F filings: institutional position changes
      - Congressional disclosures: politician buys/sells
      - Form 4 insider filings: accumulation/dumping
      - Dark pool signals: anonymous large flows

    Parameters:
        engine: SQLAlchemy engine.
        days: Lookback window.

    Returns:
        List of WealthFlow objects sorted by amount descending.
    """
    from intelligence.actor_network import WealthFlow, _ensure_tables  # lazy — avoids circular import

    _ensure_tables(engine)
    cutoff = date.today() - timedelta(days=days)
    flows: list = []

    # ── 13F-derived flows (institutional) ─────────────────────────────
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT ss.source_id, ss.ticker, ss.signal_type,
                       ss.signal_date, ss.signal_value, ss.trust_score
                FROM signal_sources ss
                WHERE ss.source_type = 'institutional'
                  AND ss.signal_date >= :cutoff
                ORDER BY ss.signal_date DESC
                LIMIT 500
            """), {"cutoff": cutoff}).fetchall()

            for r in rows:
                value_data = _parse_signal_value(r[4])
                amount = value_data.get("amount", 0) or value_data.get("market_value", 0)
                flows.append(WealthFlow(
                    from_actor=str(r[0]),
                    to_actor=str(r[1]),
                    amount_estimate=float(amount) if amount else 0,
                    confidence="confirmed" if r[5] and float(r[5]) > 0.7 else "likely",
                    evidence=["13f_filing"],
                    timestamp=str(r[3]),
                    implication=f"Institutional {r[2]} in {r[1]}",
                ))
    except Exception as exc:
        log.debug("13F flow query failed: {e}", e=str(exc))

    # ── Congressional disclosures ─────────────────────────────────────
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT source_id, ticker, signal_type,
                       signal_date, signal_value, trust_score
                FROM signal_sources
                WHERE source_type = 'congressional'
                  AND signal_date >= :cutoff
                ORDER BY signal_date DESC
                LIMIT 500
            """), {"cutoff": cutoff}).fetchall()

            for r in rows:
                value_data = _parse_signal_value(r[4])
                amount = value_data.get("amount", 0)
                low = value_data.get("amount_low", 0)
                high = value_data.get("amount_high", 0)
                if low and high:
                    amount = (float(low) + float(high)) / 2
                flows.append(WealthFlow(
                    from_actor=str(r[0]),
                    to_actor=str(r[1]),
                    amount_estimate=float(amount) if amount else 0,
                    confidence="confirmed",
                    evidence=["congressional_disclosure"],
                    timestamp=str(r[3]),
                    implication=f"Congress member {r[0]} {r[2]} {r[1]}",
                ))
    except Exception as exc:
        log.debug("Congressional flow query failed: {e}", e=str(exc))

    # ── Insider filings (Form 4) ──────────────────────────────────────
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT source_id, ticker, signal_type,
                       signal_date, signal_value, trust_score
                FROM signal_sources
                WHERE source_type = 'insider'
                  AND signal_date >= :cutoff
                ORDER BY signal_date DESC
                LIMIT 500
            """), {"cutoff": cutoff}).fetchall()

            for r in rows:
                value_data = _parse_signal_value(r[4])
                amount = value_data.get("amount", 0) or value_data.get("value", 0)
                flows.append(WealthFlow(
                    from_actor=str(r[0]),
                    to_actor=str(r[1]),
                    amount_estimate=float(amount) if amount else 0,
                    confidence="confirmed",
                    evidence=["form4"],
                    timestamp=str(r[3]),
                    implication=f"Insider {r[0]} {r[2]} {r[1]}",
                ))
    except Exception as exc:
        log.debug("Insider flow query failed: {e}", e=str(exc))

    # ── Dark pool signals ─────────────────────────────────────────────
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT ticker, signal_type, signal_date,
                       signal_value, trust_score
                FROM signal_sources
                WHERE source_type = 'darkpool'
                  AND signal_date >= :cutoff
                ORDER BY signal_date DESC
                LIMIT 200
            """), {"cutoff": cutoff}).fetchall()

            for r in rows:
                value_data = _parse_signal_value(r[3])
                volume = value_data.get("volume", 0) or value_data.get("spike_ratio", 1)
                flows.append(WealthFlow(
                    from_actor="dark_pool_anonymous",
                    to_actor=str(r[0]),
                    amount_estimate=float(volume) if volume else 0,
                    confidence="rumored",
                    evidence=["finra_ats"],
                    timestamp=str(r[2]),
                    implication=f"Dark pool {r[1]} signal in {r[0]}",
                ))
    except Exception as exc:
        log.debug("Dark pool flow query failed: {e}", e=str(exc))

    flows.sort(key=lambda f: abs(f.amount_estimate), reverse=True)
    log.info("Tracked {n} wealth flows over {d} days", n=len(flows), d=days)
    return flows


def persist_wealth_flows(
    engine: Engine,
    flows: list,
) -> int:
    """Persist WealthFlow objects to the wealth_flows table.

    Parameters:
        engine: SQLAlchemy engine.
        flows: List of WealthFlow objects to persist.

    Returns:
        Number of rows inserted.
    """
    from intelligence.actor_network import _ensure_tables  # lazy — avoids circular import

    _ensure_tables(engine)
    count = 0
    with engine.begin() as conn:
        for flow in flows:
            try:
                conn.execute(text("""
                    INSERT INTO wealth_flows
                        (from_actor, to_entity, amount_estimate,
                         confidence, evidence, flow_date, implication)
                    VALUES
                        (:from_actor, :to_entity, :amount,
                         :conf, :evidence, :flow_date, :impl)
                """), {
                    "from_actor": flow.from_actor,
                    "to_entity": flow.to_actor,
                    "amount": flow.amount_estimate,
                    "conf": flow.confidence,
                    "evidence": json.dumps(flow.evidence),
                    "flow_date": flow.timestamp[:10] if flow.timestamp else None,
                    "impl": flow.implication,
                })
                count += 1
            except Exception as exc:
                log.debug("Failed to persist flow: {e}", e=str(exc))
    log.info("Persisted {n} wealth flows", n=count)
    return count
