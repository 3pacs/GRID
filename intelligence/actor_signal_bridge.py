"""Actor Signal Bridge — injects actor intelligence into the prediction pipeline.

This module is the missing link between the actor graph and the analytical
engines. It provides functions that the oracle, trust scorer, and causation
engine call to get actor-aware context.

Without this, the prediction says "whale bought NVDA calls" but doesn't know
it was Pelosi, who has a 73% hit rate on tech trades, sits on the oversight
committee, and just visited Taiwan last month.

Functions:
    get_actor_signals_for_ticker — returns actor activity for a ticker
    get_actor_trust_weights — returns trust-weighted actor signals
    get_actor_context — returns who moved what and why (for causation)
    enrich_signal_with_actor — adds actor context to a raw signal
"""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime, time, timedelta, timezone
import math
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence.actor_identity import looks_like_identifier, PLACEHOLDER_TICKERS
from intelligence.lever_pullers import AGGREGATE_SOURCE_TYPES


# signal_data.source_id is often an event/series identifier, including direction.
# signal_type is the channel; never treat differently named rows as independent.
ACTOR_GROUPING_BASIS = "actor_signal_type_latest_date_v1"
MAX_ACTOR_OBSERVATIONS = 1000
MAX_ACTOR_AUDIT_SAMPLES = 12
MAX_ACTOR_SUPPORTING_ROWS = 20


def normalize_actor_direction(raw: Any) -> str | None:
    """Only explicit directional labels are votes; never infer from payload prose."""
    value = str(raw or "").strip().lower()
    if value in {"bull", "bullish", "buy", "long", "call", "up"}:
        return "bullish"
    if value in {"bear", "bearish", "sell", "short", "put", "down"}:
        return "bearish"
    return None


def _actor_id(name: str) -> str:
    return "_".join(name.lower().split()).replace(".", "").replace(",", "")



def _actor_score(value: Any) -> float | None:
    try:
        score = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return score if not isinstance(value, bool) and math.isfinite(score) and 0 <= score <= 1 else None

def _event_date(value: Any) -> date | None:
    # signal_date is DATE. Do not silently invent a timestamp from it.
    if isinstance(value, datetime):
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(value) if isinstance(value, str) else None
    except ValueError:
        return None


def _available_at(value: Any) -> datetime | None:
    try:
        stamp = datetime.fromisoformat(value) if isinstance(value, str) else value
    except ValueError:
        return None
    if not isinstance(stamp, datetime) or stamp.tzinfo is None or stamp.utcoffset() is None:
        return None
    return stamp.astimezone(timezone.utc)



def _source_ref(record: dict[str, Any]) -> dict[str, Any]:
    return {k: record[k] for k in ("row_id", "source_id", "signal_date", "available_at",
                                   "raw_direction", "direction")}


def _compact_audit(audit: dict[str, Any]) -> dict[str, Any]:
    """Excluded/superseded detail is sampled; used support is kept separately."""
    records = audit["observations"]
    audit["observation_count"] = len(records)
    audit["disposition_counts"] = dict(Counter(r["reason"] for r in records))
    audit["observations_complete"] = len(records) <= MAX_ACTOR_AUDIT_SAMPLES
    audit["observations_sample_limit"] = MAX_ACTOR_AUDIT_SAMPLES
    samples = []
    for r in records[:MAX_ACTOR_AUDIT_SAMPLES]:
        sample = {**_source_ref(r), "actor_id": r["actor_id"], "signal_type": r["signal_type"],
                  "status": r["status"], "reason": r["reason"],
                  "actor_trust": r["actor_trust"], "actor_influence": r["actor_influence"]}
        clipped = [key for key, value in sample.items() if isinstance(value, str) and len(value) > 160]
        for key in clipped:
            sample[key] = sample[key][:160]
        if clipped:
            sample["truncated_fields"] = clipped
            audit["observations_complete"] = False
        samples.append(sample)
    audit["observations"] = samples
    audit["candidate_window_complete"] = audit["complete"]
    return audit

def select_actor_evidence(
    rows: list[dict[str, Any]], *, as_of: datetime, days: int = 30,
) -> dict[str, Any]:
    """Select one vote per actor/channel, retaining every candidate's disposition.

    Latest event DATE wins, not latest arrival. All latest-day rows must agree
    on a known direction; an unknown/neutral row or a disagreement withholds the
    group. Ingestion time orders availability only, never same-day actions.
    Date-only evidence has unknown exact age and an explicitly labelled lower
    bound. Current actor attributes are not a historical score reconstruction.
    """
    now = _available_at(as_of)
    if now is None:
        raise ValueError("as_of must be timezone-aware")
    if days < 0:
        raise ValueError("days must be non-negative")
    cutoff = now.date() - timedelta(days=days)
    audit: dict[str, Any] = {
        "as_of": now.isoformat(), "grouping_basis": ACTOR_GROUPING_BASIS,
        "actor_attributes_basis": "current_actor_table_not_historical",
        "signals": [], "observations": [], "complete": True,
    }
    overflow = len(rows) > MAX_ACTOR_OBSERVATIONS
    if overflow:
        # A truncated window could hide a contradiction. Keep bounded lineage,
        # but never choose a directional vote from incomplete observations.
        audit.update(complete=False, reason="candidate_limit_exceeded",
                     candidate_count_lower_bound=len(rows))
        rows = rows[:MAX_ACTOR_OBSERVATIONS + 1]
    groups: dict[tuple[str, str], list[tuple[dict, dict]]] = {}
    for row in rows:
        actor = " ".join(str(row.get("actor") or "").split())
        channel = str(row.get("signal_type") or "").strip().lower()
        event_date = _event_date(row.get("signal_date"))
        available = _available_at(row.get("created_at"))
        direction = normalize_actor_direction(row.get("direction"))
        trust = _actor_score(row.get("actor_trust", 0.5))
        influence = _actor_score(row.get("actor_influence", 0.3))
        record = {
            "row_id": row.get("id"), "source_id": row.get("source_id"),
            "actor": actor, "actor_id": _actor_id(actor), "signal_type": channel,
            "actor_trust": trust, "actor_influence": influence,
            "raw_direction": row.get("direction"), "direction": direction,
            "signal_date": event_date.isoformat() if event_date else None,
            "event_time_basis": "signal_date_date_only" if event_date else "unknown",
            "available_at": available.isoformat() if available else None,
            "availability_basis": "signal_data.created_at_ingestion" if available else "unknown",
            "freshness_hours": None, "freshness_basis": "exact_event_time_unknown",
            "age_lower_bound_basis": "end_of_signal_date_utc_calendar_not_event_timestamp",
            "date_age_days": (now.date() - event_date).days if event_date else None,
            "freshness_hours_lower_bound": (
                max(0.0, (now - datetime.combine(event_date + timedelta(days=1), time.min,
                                                tzinfo=timezone.utc)).total_seconds() / 3600)
                if event_date and event_date <= now.date() else None
            ),
            "status": "excluded", "reason": None,
        }
        audit["observations"].append(record)
        reason = None
        if (not actor or actor.upper() in PLACEHOLDER_TICKERS
                or looks_like_identifier(actor) or actor.lower().startswith("qq_")):
            reason = "non_actor_identity"
        elif channel in AGGREGATE_SOURCE_TYPES:
            reason = "aggregate_source"
        elif not channel:
            reason = "missing_source_channel"
        elif event_date is None:
            reason = "unknown_event_date"
        elif event_date > now.date():
            reason = "future_event_date"
        elif event_date < cutoff:
            reason = "outside_lookback"
        elif available is None:
            reason = "unknown_availability"
        elif available > now:
            reason = "not_yet_available"
        elif trust is None or influence is None:
            reason = "invalid_actor_scores"
        if reason:
            record["reason"] = reason
            continue
        groups.setdefault((_actor_id(actor), channel), []).append((row, record))

    if overflow:
        for record in audit["observations"]:
            if record["reason"] is None:
                record["reason"] = "candidate_limit_exceeded"
        return _compact_audit(audit)

    for key, members in sorted(groups.items()):
        latest = max(record["signal_date"] for _, record in members)
        current = [(row, record) for row, record in members if record["signal_date"] == latest]
        directions = {record["direction"] for _, record in current}
        for _, record in members:
            record["reason"] = "superseded_event_date"
        if None in directions or len(directions) != 1:
            reason = "unknown_latest_direction" if None in directions else "conflicting_latest_directions"
            for _, record in current:
                record["reason"] = reason
            continue
        if len(current) > MAX_ACTOR_SUPPORTING_ROWS:
            # Never store an incomplete list of the rows used for a vote.
            for _, record in current:
                record["reason"] = "supporting_lineage_limit_exceeded"
            continue
        # Stable representative only, not an assertion of event order. Every
        # latest-day row supports the same vote and is retained in provenance.
        current.sort(key=lambda pair: (str(pair[1]["row_id"]), str(pair[1]["source_id"])))
        row, selected = current[0]
        for _, record in current:
            record["reason"] = "same_day_correlated_observation"
        selected.update(status="selected", reason="latest_actor_channel_vote")
        provenance = {
            **selected, "grouping_basis": ACTOR_GROUPING_BASIS,
            "group_key": list(key),
            "supporting_row_ids": [r["row_id"] for _, r in current],
            "supporting_rows": [_source_ref(r) for _, r in current],
            "supporting_rows_complete": True,
            "observation_count": len(members),
            "superseded_count": len(members) - len(current),
        }
        audit["signals"].append({
            **row, "actor": selected["actor"], "direction": selected["direction"],
            "signal_type": selected["signal_type"],
            "signal_date": latest, "provenance": provenance,
            "actor_trust": selected["actor_trust"], "actor_influence": selected["actor_influence"],
        })
    audit["signals"].sort(key=lambda s: (-s.get("actor_influence", 0.3),
                                         _actor_id(s["actor"]), s["signal_type"]))
    return _compact_audit(audit)


def get_actor_evidence_for_ticker(
    engine: Engine, ticker: str, days: int = 30, *, as_of: datetime | None = None,
) -> dict[str, Any]:
    """Read bounded actor candidates available at the decision instant.

    signal_data has no vintage history and is not a resolved_series PITStore
    table. These bound parameters enforce its DATE/ingestion-time boundaries;
    they cannot reconstruct later edits or historical actors-table scores.
    Unknown dates/availability are read only to expose exclusion reasons.
    """
    now = _available_at(as_of if as_of is not None else datetime.now(timezone.utc))
    if now is None:
        raise ValueError("as_of must be timezone-aware")
    if days < 0:
        raise ValueError("days must be non-negative")
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, signal_type, actor, direction, magnitude, confidence,
                   signal_date, data, source_id, created_at
            FROM signal_data
            WHERE ticker = :ticker
              AND actor IS NOT NULL AND actor != ''
              AND (signal_date BETWEEN :cutoff AND :as_of_date OR signal_date IS NULL)
              AND (created_at <= :as_of OR created_at IS NULL)
            ORDER BY signal_date DESC, id DESC
            LIMIT :candidate_limit
        """), {"ticker": ticker, "cutoff": now.date() - timedelta(days=days),
                "as_of_date": now.date(), "as_of": now,
                "candidate_limit": MAX_ACTOR_OBSERVATIONS + 1}).mappings().all()
        candidates = [dict(row) for row in rows]
        if len(candidates) > MAX_ACTOR_OBSERVATIONS:
            return select_actor_evidence(candidates, as_of=now, days=days)
        actor_ids = sorted({_actor_id(str(r["actor"])) for r in candidates})
        actor_lookup: dict[str, dict] = {}
        if actor_ids:
            actor_rows = conn.execute(text(
                "SELECT id, trust_score, influence_score, category, tier "
                "FROM actors WHERE id = ANY(:ids)"
            ), {"ids": actor_ids}).fetchall()
            for ar in actor_rows:
                actor_lookup[ar[0]] = {
                    "actor_trust": float(ar[1]) if ar[1] is not None else 0.5,
                    "actor_influence": float(ar[2]) if ar[2] is not None else 0.3,
                    "actor_category": ar[3] or "unknown", "actor_tier": ar[4] or "unknown",
                }
    for row in candidates:
        row.update({"actor_trust": 0.5, "actor_influence": 0.3,
                    "actor_category": "unknown", "actor_tier": "unknown"})
        row.update(actor_lookup.get(_actor_id(str(row["actor"])), {}))
    audit = select_actor_evidence(candidates, as_of=now, days=days)
    audit["query_scope"] = "ticker_lookback_event_date_and_ingestion_bounded"
    return audit


def get_actor_signals_for_ticker(
    engine: Engine, ticker: str, days: int = 30, *, as_of: datetime | None = None,
) -> list[dict[str, Any]]:
    """Eligible directional actor/channel votes, with full selection lineage."""
    return get_actor_evidence_for_ticker(engine, ticker, days, as_of=as_of)["signals"]


def get_actor_trust_weights(
    engine: Engine,
    ticker: str,
    days: int = 90,
) -> dict[str, float]:
    """Get actor-weighted trust scores for a ticker.

    Returns a dict of source_module → trust_weight that the oracle's
    signal_aggregator can use. Actors with higher trust_score and
    influence_score get more weight.

    This replaces the flat trust_scores dict with one that knows
    Pelosi's congressional trades are more predictive than random_whale.
    """
    cutoff = date.today() - timedelta(days=days)

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT signal_type, actor
            FROM signal_data
            WHERE ticker = :ticker
              AND actor IS NOT NULL AND actor != ''
              AND signal_date >= :cutoff
        """), {"ticker": ticker, "cutoff": cutoff}).fetchall()

        # Batch actor lookup
        actor_names = list({r[1] for r in rows if r[1]})
        actor_lookup: dict[str, tuple] = {}
        if actor_names:
            aids = [n.strip().lower().replace(" ", "_").replace(".", "").replace(",", "") for n in actor_names]
            arows = conn.execute(text(
                "SELECT id, trust_score, influence_score FROM actors WHERE id = ANY(:ids)"
            ), {"ids": aids}).fetchall()
            for ar in arows:
                actor_lookup[ar[0]] = (float(ar[1]) if ar[1] else 0.5, float(ar[2]) if ar[2] else 0.3)

    # Weight = trust × influence for each signal type
    weights: dict[str, list[float]] = {}
    for sig_type, actor_name in rows:
        aid = actor_name.strip().lower().replace(" ", "_").replace(".", "").replace(",", "")
        trust, influence = actor_lookup.get(aid, (0.5, 0.3))
        key = sig_type
        if key not in weights:
            weights[key] = []
        weights[key].append(trust * influence)

    # Average per signal type
    return {k: sum(v) / len(v) for k, v in weights.items() if v}


def get_actor_context_for_causation(
    engine: Engine,
    ticker: str,
    days: int = 14,
) -> list[dict[str, Any]]:
    """Get the causal actor chain for a ticker move.

    Returns actors who traded/influenced this ticker recently,
    ordered by influence × recency. This is what the causation engine
    should call to answer "who caused this move?"

    Each entry includes the actor's connections to other entities
    (board seats, lobbying targets, political affiliations).
    """
    cutoff = date.today() - timedelta(days=days)

    with engine.connect() as conn:
        # Step 1: Get signals
        sig_rows = conn.execute(text("""
            SELECT DISTINCT actor, signal_type, direction, signal_date
            FROM signal_data
            WHERE ticker = :ticker
              AND actor IS NOT NULL AND actor != ''
              AND signal_date >= :cutoff
            ORDER BY signal_date DESC
            LIMIT 20
        """), {"ticker": ticker, "cutoff": cutoff}).fetchall()

        # Step 2: Batch actor lookup
        actor_names = list({r[0] for r in sig_rows})
        actor_ids = [n.strip().lower().replace(" ", "_").replace(".", "").replace(",", "") for n in actor_names]
        actor_lookup: dict[str, dict] = {}
        if actor_ids:
            arows = conn.execute(text(
                "SELECT id, trust_score, influence_score, category, title, tier "
                "FROM actors WHERE id = ANY(:ids)"
            ), {"ids": actor_ids}).fetchall()
            for ar in arows:
                actor_lookup[ar[0]] = {
                    "trust": float(ar[1]) if ar[1] else 0.5,
                    "influence": float(ar[2]) if ar[2] else 0.3,
                    "category": ar[3] or "unknown",
                    "title": ar[4] or "",
                    "tier": ar[5] or "unknown",
                }

        # Step 3: Batch connection lookup for all actors
        conn_lookup: dict[str, list] = {}
        if actor_ids:
            crows = conn.execute(text(
                "SELECT actor_a, actor_b, relationship, strength "
                "FROM actor_connections WHERE actor_a = ANY(:ids) "
                "ORDER BY strength DESC"
            ), {"ids": actor_ids}).fetchall()
            for c in crows:
                if c[0] not in conn_lookup:
                    conn_lookup[c[0]] = []
                if len(conn_lookup[c[0]]) < 10:
                    conn_lookup[c[0]].append({
                        "target": c[1], "relationship": c[2],
                        "strength": float(c[3]) if c[3] else 0,
                    })

        results = []
        for r in sig_rows:
            actor_name = r[0]
            aid = actor_name.strip().lower().replace(" ", "_").replace(".", "").replace(",", "")
            ainfo = actor_lookup.get(aid, {})
            results.append({
                "actor": actor_name,
                "signal_type": r[1],
                "direction": r[2],
                "signal_date": str(r[3]),
                "trust_score": ainfo.get("trust", 0.5),
                "influence_score": ainfo.get("influence", 0.3),
                "category": ainfo.get("category", "unknown"),
                "title": ainfo.get("title", ""),
                "tier": ainfo.get("tier", "unknown"),
                "other_connections": conn_lookup.get(aid, []),
            })

        results.sort(key=lambda x: x["influence_score"], reverse=True)

    return results


def enrich_signals_with_actors(
    engine: Engine,
    signals: list[dict[str, Any]],
    ticker: str,
) -> list[dict[str, Any]]:
    """Enrich a list of signals with actor context.

    Called by the oracle engine before aggregation. Adds actor_trust
    and actor_influence fields to each signal that has an actor.
    Signals without actors pass through unchanged.
    """
    actor_signals = get_actor_signals_for_ticker(engine, ticker, days=30)

    # Build lookup: (signal_type, direction) → best actor info
    actor_lookup: dict[str, dict] = {}
    for asig in actor_signals:
        key = f"{asig['signal_type']}:{asig['direction']}"
        if key not in actor_lookup or asig["actor_influence"] > actor_lookup[key].get("actor_influence", 0):
            actor_lookup[key] = asig

    enriched = []
    for sig in signals:
        sig_copy = {**sig}
        key = f"{sig.get('source_module', '')}:{normalize_actor_direction(sig.get('direction'))}"
        actor_info = actor_lookup.get(key)
        if actor_info:
            sig_copy["actor_name"] = actor_info["actor"]
            sig_copy["actor_trust"] = actor_info["actor_trust"]
            sig_copy["actor_influence"] = actor_info["actor_influence"]
            sig_copy["actor_category"] = actor_info["actor_category"]
            # Boost trust_score by actor credibility
            existing_trust = float(sig_copy.get("trust_score", 0.5))
            sig_copy["trust_score"] = min(0.95, existing_trust * (0.5 + actor_info["actor_trust"]))
        enriched.append(sig_copy)

    return enriched


def sync_actor_trust_to_signal_sources(engine: Engine) -> int:
    """Push actor trust scores into signal_sources for the trust scorer.

    The trust_scorer reads from signal_sources.trust_score but never
    checks the actors table. This bridges the gap by copying
    actor trust scores back into signal_sources rows.
    """
    with engine.connect() as conn:
        result = conn.execute(text("""
            UPDATE signal_sources ss
            SET trust_score = a.trust_score
            FROM actors a
            WHERE LOWER(ss.source_id) = LOWER(a.name)
              AND a.trust_score IS NOT NULL
              AND a.trust_score != COALESCE(ss.trust_score, 0.5)
        """))
        updated = result.rowcount
        conn.commit()

    if updated > 0:
        log.info("Synced {n} actor trust scores to signal_sources", n=updated)
    return updated
