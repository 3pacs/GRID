"""
GRID Intelligence — Actor network analysis functions.

Contains:
    get_actor_context_for_ticker    — who cares about this stock?
    enrich_lever_pullers_with_actors — cross-reference lever_pullers
    generate_actor_report           — comprehensive intelligence report
"""

from __future__ import annotations

import json
import re
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence.actors.db import _ensure_tables, _load_actors_from_db
from intelligence.actors.graph import build_actor_graph
from intelligence.actors.models import Actor
from intelligence.lever_pullers import puller_identity


_TICKER_CONTEXT_EXCLUDED = ("icij_entity", "icij_officer", "icij_intermediary")
_NAME_TOKEN = re.compile(r"[\w&.'-]+", re.UNICODE)
_MIN_FRAGMENT_CHARS = 4
_MAX_FRAGMENT_WORDS = 6


def _name_fragments(names: set[str]) -> list[str]:
    """Lower-cased names plus their contiguous word runs (>= 4 chars).

    The old matcher kept actor X when ``X.name.lower()`` was a substring of a
    signal-source name ("Nancy Pelosi" in "Hon. Nancy Pelosi (D-CA)"). Word
    runs of each source name reproduce that at word granularity and turn it
    into an indexed equality lookup.
    """
    out: set[str] = set()
    for raw in names:
        low = raw.strip().lower()
        if not low:
            continue
        out.add(low)
        tokens = _NAME_TOKEN.findall(re.sub(r"\([^)]*\)", " ", low))
        for i in range(len(tokens)):
            for j in range(i + 1, min(len(tokens), i + _MAX_FRAGMENT_WORDS) + 1):
                frag = " ".join(tokens[i:j])
                if len(frag) >= _MIN_FRAGMENT_CHARS:
                    out.add(frag)
    return sorted(out)


def _match_actors_for_ticker(conn: Any, ticker: str, actor_names: set[str]) -> list[dict]:
    """Actors behind a ticker's recent signals, plus its insider flows.

    1. Name match: ``lower(actors.name) = ANY(fragments)`` (idx_actors_name_lower).
    2. Positions: actors with ``wealth_flows`` into ``corp_<TICKER>`` (Form 4
       insiders -> company; ``idx_wealth_flows_to_entity``). This replaces the
       ``known_positions`` scan, which is empty for all ~3.49M actors on
       griddb (2026-11-06) and cannot be indexed cheaply.
    """
    def _row(r: Any) -> dict:
        return {
            "id": r[0],
            "name": r[1],
            "tier": r[2],
            "category": r[3],
            "title": r[4] or "",
            "influence": round(float(r[5]) if r[5] is not None else 0.5, 3),
            "motivation": r[6] or "unknown",
            "trust_score": round(float(r[7]) if r[7] is not None else 0.5, 3),
        }

    matched: dict[str, dict] = {}
    fragments = _name_fragments(actor_names)
    if fragments:
        rows = conn.execute(text("""
            SELECT id, name, tier, category, title, influence_score,
                   motivation_model, trust_score
            FROM actors
            WHERE lower(name) = ANY(:names)
              AND category <> ALL(:excluded)
            ORDER BY influence_score DESC NULLS LAST
            LIMIT 200
        """), {"names": fragments, "excluded": list(_TICKER_CONTEXT_EXCLUDED)}).fetchall()
        for r in rows:
            matched.setdefault(r[0], _row(r))

    t = ticker.strip()
    corp_ids = sorted({f"corp_{t.upper()}", f"corp_{t.lower()}", f"corp_{t}"})
    rows = conn.execute(text("""
        SELECT a.id, a.name, a.tier, a.category, a.title, a.influence_score,
               a.motivation_model, a.trust_score
        FROM (
            SELECT from_actor, MAX(amount_estimate) AS amt
            FROM wealth_flows
            WHERE to_entity = ANY(:corp)
            GROUP BY from_actor
            ORDER BY amt DESC NULLS LAST
            LIMIT 50
        ) w
        JOIN actors a ON a.id = w.from_actor
        WHERE a.category <> ALL(:excluded)
    """), {"corp": corp_ids, "excluded": list(_TICKER_CONTEXT_EXCLUDED)}).fetchall()
    for r in rows:
        matched.setdefault(r[0], _row(r))
    return list(matched.values())


def get_actor_context_for_ticker(
    engine: Engine,
    ticker: str,
) -> dict:
    """Get all actor intelligence relevant to a specific ticker.

    For watchlist detail pages: which actors are relevant to this ticker,
    their recent actions, motivations, and connections.

    Parameters:
        engine: SQLAlchemy engine.
        ticker: Stock ticker symbol (e.g. "AAPL").

    Returns:
        Dict with keys: ticker, actors, recent_actions, power_summary,
        risk_signals.
    """
    _ensure_tables(engine)
    result: dict[str, Any] = {
        "ticker": ticker,
        "actors": [],
        "recent_actions": [],
        "power_summary": "",
        "risk_signals": [],
    }

    cutoff = date.today() - timedelta(days=60)

    try:
        with engine.connect() as conn:
            # Find all signal sources acting on this ticker
            rows = conn.execute(text("""
                SELECT source_type, source_id, signal_type,
                       signal_date, signal_value, trust_score
                FROM signal_sources
                WHERE ticker = :t
                  AND signal_date >= :cutoff
                ORDER BY signal_date DESC
                LIMIT 100
            """), {"t": ticker, "cutoff": cutoff}).fetchall()

            actor_names: set[str] = set()
            buys = 0
            sells = 0

            for r in rows:
                source_type = str(r[0])
                # The actor behind the row: QuiverQuant source_ids are per act.
                source_id = str(puller_identity(source_type, str(r[1]), r[4]))
                direction = str(r[2])
                sig_date = str(r[3])
                trust = float(r[5]) if r[5] else 0.5

                if direction.upper() == "BUY":
                    buys += 1
                elif direction.upper() == "SELL":
                    sells += 1

                actor_names.add(source_id)
                result["recent_actions"].append({
                    "source_type": source_type,
                    "actor": source_id,
                    "direction": direction,
                    "date": sig_date,
                    "trust_score": round(trust, 3),
                })

            # Cross-reference with known actors through indexed lookups
            # (lower(name) btree + wealth_flows.to_entity), never by loading
            # the actor table: the old full scan pulled ~1.86M actors per call.
            matched_actors = _match_actors_for_ticker(conn, ticker, actor_names)

            result["actors"] = sorted(
                matched_actors,
                key=lambda x: x.get("influence", 0),
                reverse=True,
            )

            # Power summary
            total_actors = len(actor_names)
            if total_actors > 0:
                net_direction = (
                    "bullish" if buys > sells
                    else "bearish" if sells > buys
                    else "neutral"
                )
                result["power_summary"] = (
                    f"{total_actors} actors active on {ticker} in the last 60 days. "
                    f"Net bias: {net_direction} ({buys} buys, {sells} sells). "
                    f"{len(matched_actors)} matched to known power players."
                )
            else:
                result["power_summary"] = (
                    f"No recent actor activity detected for {ticker}."
                )

            # Risk signals: check for insider cluster sells, congressional sells
            if sells >= 3 and sells > buys * 2:
                result["risk_signals"].append({
                    "signal": "cluster_selling",
                    "description": f"{sells} sells vs {buys} buys from tracked actors",
                    "severity": "high",
                })

            # Check for high-influence actor selling
            for actor_info in matched_actors:
                if actor_info["influence"] > 0.8:
                    for action in result["recent_actions"]:
                        if (
                            action["actor"] == actor_info["name"]
                            and action["direction"].upper() == "SELL"
                        ):
                            result["risk_signals"].append({
                                "signal": "high_influence_sell",
                                "description": (
                                    f"{actor_info['name']} ({actor_info['title']}) "
                                    f"selling {ticker}"
                                ),
                                "severity": "critical",
                            })
                            break
    except Exception as exc:
        log.warning("Actor context for {t} failed: {e}", t=ticker, e=str(exc))

    return result


def enrich_lever_pullers_with_actors(engine: Engine) -> int:
    """Cross-reference lever_pullers with the actor network.

    Updates lever_pullers with actor metadata (connections, influence
    propagation, motivation_model) from the actors table.

    Returns:
        Number of lever pullers enriched.
    """
    _ensure_tables(engine)
    enriched = 0
    try:
        actors = _load_actors_from_db(engine)
        with engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT id, source_id, name FROM lever_pullers
            """)).fetchall()

            for r in rows:
                lp_id = r[0]
                source_id = str(r[1])
                lp_name = str(r[2])

                # Try to match to an actor by name
                matched_actor: Actor | None = None
                for actor_id, actor in actors.items():
                    if (
                        actor.name.lower() == lp_name.lower()
                        or actor.name.lower() in source_id.lower()
                        or source_id.lower() in actor.name.lower()
                    ):
                        matched_actor = actor
                        break

                if matched_actor:
                    conn.execute(text("""
                        UPDATE lever_pullers SET
                            influence_rank = :inf,
                            motivation_model = :mot,
                            metadata = COALESCE(metadata, '{}'::JSONB) || :meta,
                            updated_at = NOW()
                        WHERE id = :id
                    """), {
                        "inf": matched_actor.influence_score,
                        "mot": matched_actor.motivation_model,
                        "meta": json.dumps({
                            "actor_id": matched_actor.id,
                            "tier": matched_actor.tier,
                            "credibility": matched_actor.credibility,
                            "connections_count": len(matched_actor.connections),
                        }),
                        "id": lp_id,
                    })
                    enriched += 1
    except Exception as exc:
        log.warning("Lever puller enrichment failed: {e}", e=str(exc))

    log.info("Enriched {n} lever pullers with actor network data", n=enriched)
    return enriched


def generate_actor_report(engine: Engine) -> dict:
    """Generate a comprehensive actor network intelligence report.

    Combines: actor graph, wealth migration, pocket-lining flags,
    and lever puller convergence into a single actionable report.

    Parameters:
        engine: SQLAlchemy engine.

    Returns:
        Dict with keys: graph, wealth_flows, pocket_lining, convergence,
        narrative, generated_at.
    """
    log.info("Generating comprehensive actor network report")

    graph = build_actor_graph(engine)

    # Lazy import to avoid circular dependency
    from intelligence.actor_network import track_wealth_migration, assess_pocket_lining

    flows = track_wealth_migration(engine, days=90)
    flags = assess_pocket_lining(engine)

    # Attempt lever puller convergence
    convergence: list[dict] = []
    try:
        from intelligence.lever_pullers import find_lever_convergence
        convergence = find_lever_convergence(engine)
    except Exception as exc:
        log.debug("Lever convergence unavailable: {e}", e=str(exc))

    # Build narrative
    narrative_parts: list[str] = []
    narrative_parts.append(
        f"Actor network: {graph['metadata']['total_actors']} tracked entities "
        f"across {len(graph['metadata'].get('tier_breakdown', {}))} tiers."
    )

    if flows:
        top_flows = flows[:5]
        flow_summary = ", ".join(
            f"{f.from_actor}->{f.to_actor} (${f.amount_estimate:,.0f})"
            for f in top_flows if f.amount_estimate > 0
        )
        if flow_summary:
            narrative_parts.append(f"Top wealth flows: {flow_summary}.")

    if flags:
        narrative_parts.append(
            f"ALERT: {len(flags)} pocket-lining flags detected. "
            f"Most severe: {flags[0].get('detection', 'unknown')} — "
            f"{flags[0].get('who', 'unknown')}."
        )

    if convergence:
        narrative_parts.append(
            f"{len(convergence)} lever puller convergence events detected."
        )

    report = {
        "graph": graph,
        "wealth_flows": [asdict(f) for f in flows[:100]],
        "pocket_lining": flags,
        "convergence": convergence[:20] if convergence else [],
        "narrative": " ".join(narrative_parts),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    log.info("Actor network report complete")
    return report
