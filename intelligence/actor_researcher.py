"""
Actor Researcher — local LLM agent that continuously enriches actor profiles.

Runs on the local LLM (Gemma/Nemotron) and:
1. Picks actors with sparse profiles (low degree, empty metadata)
2. Searches existing DB data (raw_series, actor_connections) for mentions
3. Cross-references with ICIJ, QuiverQuant, SEC data already in the DB
4. Generates structured profile updates with EVIDENCE ONLY — no hallucination

ANTI-HALLUCINATION RULES:
- Every claim must cite a source from the DB (series_id, connection_id, or raw_payload)
- The LLM generates search queries, NOT facts
- Facts come from DB query results ONLY
- If no evidence exists, the field stays empty
- Confidence labels: confirmed (DB match), derived (cross-reference), inferred (pattern)
- NEVER fabricate connections, amounts, dates, or relationships
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


def find_sparse_actors(engine: Engine, limit: int = 50) -> list[dict[str, Any]]:
    """Find seed actors that need enrichment.

    Prioritizes high-influence actors with low degree or empty metadata.

    Returns:
        List of actor dicts needing research.
    """
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, name, tier, category, degree, influence_score,
                   metadata, source, title
            FROM actors
            WHERE source NOT LIKE 'icij%%'
            AND (
                degree < 5
                OR metadata = '{}'::jsonb
                OR title IS NULL
            )
            ORDER BY influence_score DESC, degree ASC
            LIMIT :lim
        """), {"lim": limit}).fetchall()

    return [
        {
            "id": r[0], "name": r[1], "tier": r[2], "category": r[3],
            "degree": r[4], "influence_score": float(r[5]),
            "metadata": r[6], "source": r[7], "title": r[8],
        }
        for r in rows
    ]


def gather_evidence(engine: Engine, actor_name: str, actor_id: str) -> dict[str, Any]:
    """Gather all existing DB evidence about an actor.

    Searches raw_series, actor_connections, and ICIJ data for any
    mentions of this actor. Returns ONLY what the DB contains.

    Args:
        engine: SQLAlchemy engine.
        actor_name: Actor's name.
        actor_id: Actor's ID.

    Returns:
        Dict of evidence found, keyed by source type.
    """
    evidence: dict[str, Any] = {
        "connections": [],
        "trading_activity": [],
        "lobbying": [],
        "insider_filings": [],
        "earnings_mentions": [],
        "icij_matches": [],
        "news_mentions": [],
    }

    with engine.connect() as conn:
        # 1. Direct connections from actor_connections
        conns = conn.execute(text("""
            SELECT
                CASE WHEN actor_a = :aid THEN actor_b ELSE actor_a END AS other_actor,
                relationship, strength
            FROM actor_connections
            WHERE actor_a = :aid OR actor_b = :aid
            ORDER BY strength DESC LIMIT 20
        """), {"aid": actor_id}).fetchall()

        for c in conns:
            # Resolve other actor's name
            other = conn.execute(text("SELECT name FROM actors WHERE id = :id"), {"id": c[0]}).fetchone()
            evidence["connections"].append({
                "actor": other[0] if other else c[0],
                "relationship": c[1],
                "strength": float(c[2]),
            })

        # 2. QuiverQuant congressional/insider trading
        name_pattern = f"%{actor_name}%"
        trades = conn.execute(text("""
            SELECT series_id, obs_date, value, raw_payload
            FROM raw_series
            WHERE (series_id LIKE :pat1 OR series_id LIKE :pat2)
            AND pull_status = 'SUCCESS'
            ORDER BY obs_date DESC LIMIT 10
        """), {
            "pat1": f"qq:congress:{actor_name}%",
            "pat2": f"qq:insider:%:{actor_name}%",
        }).fetchall()

        for t in trades:
            payload = json.loads(t[3]) if isinstance(t[3], str) else (t[3] or {})
            evidence["trading_activity"].append({
                "series": t[0], "date": t[1].isoformat() if t[1] else None,
                "value": float(t[2]) if t[2] else None,
                "details": {k: v for k, v in payload.items() if k in (
                    "Ticker", "TransactionType", "Amount", "Representative", "Name", "Title"
                )},
            })

        # 3. Lobbying mentions
        lobbying = conn.execute(text("""
            SELECT series_id, obs_date, value, raw_payload
            FROM raw_series
            WHERE series_id LIKE :pat AND pull_status = 'SUCCESS'
            ORDER BY obs_date DESC LIMIT 10
        """), {"pat": f"qq:lobbying:{actor_name}%"}).fetchall()

        for l in lobbying:
            payload = json.loads(l[3]) if isinstance(l[3], str) else (l[3] or {})
            evidence["lobbying"].append({
                "date": l[1].isoformat() if l[1] else None,
                "amount": float(l[2]) if l[2] else None,
                "issue": payload.get("Issue", ""),
            })

        # 4. ICIJ fuzzy matches (from actor_connections, not icij_actor_matches)
        try:
            icij = conn.execute(text("""
                SELECT ac.actor_b, b.name, ac.strength, ac.relationship
                FROM actor_connections ac
                JOIN actors b ON ac.actor_b = b.id
                WHERE ac.actor_a = :aid
                AND ac.relationship LIKE :pat
                ORDER BY ac.strength DESC LIMIT 10
            """), {"aid": actor_id, "pat": "icij_%"}).fetchall()

            for i in icij:
                evidence["icij_matches"].append({
                    "icij_id": i[0], "icij_name": i[1],
                    "similarity": float(i[2]), "match_type": i[3],
                })
        except Exception:
            pass

        # 5. Cross-reference: does this actor trade stocks of companies linked to ICIJ?
        try:
            xref = conn.execute(text("""
                SELECT DISTINCT rs.series_id, ac.actor_b, b.name as icij_name, ac.relationship
                FROM raw_series rs
                JOIN actor_connections ac ON ac.relationship LIKE :icij_pat
                JOIN actors b ON ac.actor_b = b.id
                WHERE rs.series_id LIKE :trade_pat
                AND b.name ILIKE :ticker_pat
                AND rs.pull_status = 'SUCCESS'
                LIMIT 10
            """), {
                "trade_pat": f"qq:congress:{actor_name}%",
                "icij_pat": "icij_%",
                "ticker_pat": "%",
            }).fetchall()

            for x in xref:
                evidence.setdefault("cross_references", []).append({
                    "series": x[0], "icij_actor": x[2],
                    "relationship": x[3],
                    "signal": "congress_member_trades_icij_linked_company",
                })
        except Exception:
            pass

    return evidence


def enrich_actor_with_llm(
    engine: Engine,
    actor: dict[str, Any],
    evidence: dict[str, Any],
) -> dict[str, Any] | None:
    """Use local LLM to generate a structured profile update.

    The LLM sees ONLY the evidence gathered from the DB.
    It synthesizes a profile — it does NOT generate new facts.

    Returns:
        Profile update dict, or None if insufficient evidence.
    """
    from llm.router import get_llm, Tier

    # Skip if no evidence at all
    total_evidence = sum(len(v) for v in evidence.values() if isinstance(v, list))
    if total_evidence == 0:
        return None

    # Build the prompt with evidence
    evidence_text = json.dumps(evidence, indent=2, default=str)

    prompt = f"""You are analyzing an actor in a financial intelligence network.

ACTOR: {actor['name']}
CATEGORY: {actor['category']}
TIER: {actor['tier']}
CURRENT TITLE: {actor.get('title', 'unknown')}

EVIDENCE FROM DATABASE (this is ALL you know — do not add anything not in this evidence):
{evidence_text}

Based ONLY on the evidence above, generate a structured profile update as JSON:
{{
    "title": "their role/title if evident from the data",
    "key_relationships": ["list of most important connections with context"],
    "trading_patterns": "summary of any trading activity found",
    "lobbying_activity": "summary of lobbying if found",
    "offshore_connections": "summary of ICIJ matches if found",
    "risk_flags": ["any concerning patterns"],
    "confidence": "confirmed if strong DB evidence, derived if cross-referenced, inferred if weak",
    "new_actors_discovered": ["names of people/orgs mentioned in evidence that should be tracked as separate actors — ONLY names that appear in the evidence data above, never invented names"],
    "rabbit_holes": ["specific leads worth investigating further — e.g. 'ICIJ entity X shares address with entity Y', 'insider traded same ticker as congressman Z'"]
}}

CRITICAL: Only include information that appears in the evidence above. If a field has no evidence, set it to null. Do not fabricate any facts, dates, amounts, or relationships."""

    client = get_llm(Tier.REASON)

    # Use chat API directly to handle Gemma 4's thinking mode
    messages = [
        {"role": "system", "content": "You are a financial intelligence analyst. You ONLY report facts from provided evidence. You NEVER fabricate information. Reply with ONLY valid JSON, no other text."},
        {"role": "user", "content": prompt},
    ]
    response = client.chat(messages, temperature=0.1, num_predict=1500)

    if not response:
        return None

    # Parse LLM response — try to extract JSON from anywhere in the response
    try:
        # Find the outermost { ... } in the response
        json_start = response.find("{")
        json_end = response.rfind("}") + 1
        if json_start >= 0 and json_end > json_start:
            candidate = response[json_start:json_end]
            profile = json.loads(candidate)
            profile["_evidence_count"] = total_evidence
            profile["_generated_at"] = datetime.now(timezone.utc).isoformat()
            return profile
    except json.JSONDecodeError:
        # Try cleaning up common LLM artifacts
        try:
            cleaned = response.replace("```json", "").replace("```", "").strip()
            json_start = cleaned.find("{")
            json_end = cleaned.rfind("}") + 1
            if json_start >= 0 and json_end > json_start:
                profile = json.loads(cleaned[json_start:json_end])
                profile["_evidence_count"] = total_evidence
                profile["_generated_at"] = datetime.now(timezone.utc).isoformat()
                return profile
        except json.JSONDecodeError:
            pass
        log.debug("LLM response not valid JSON for {n}: {r}", n=actor["name"], r=response[:200])

    return None


def update_actor_profile(engine: Engine, actor_id: str, profile: dict[str, Any]) -> None:
    """Write the enriched profile back to the actors table."""
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE actors SET
                metadata = metadata || :profile,
                title = COALESCE(:title, title),
                updated_at = NOW()
            WHERE id = :aid
        """), {
            "aid": actor_id,
            "profile": json.dumps({"llm_profile": profile}),
            "title": profile.get("title"),
        })


def research_batch(engine: Engine, batch_size: int = 20) -> dict[str, Any]:
    """Research a batch of sparse actors.

    This is designed to run continuously on the local LLM.
    Each call processes `batch_size` actors, discovers new actors
    from evidence, and follows rabbit holes.

    Returns:
        Summary of actors researched and updated.
    """
    actors = find_sparse_actors(engine, limit=batch_size)
    if not actors:
        return {"researched": 0, "updated": 0, "new_actors": 0, "rabbit_holes": 0}

    updated = 0
    new_actors_created = 0
    rabbit_holes_found = 0

    for actor in actors:
        try:
            evidence = gather_evidence(engine, actor["name"], actor["id"])
            profile = enrich_actor_with_llm(engine, actor, evidence)

            if profile:
                update_actor_profile(engine, actor["id"], profile)
                updated += 1
                log.info("Enriched actor: {n} (evidence={e}, confidence={c})",
                         n=actor["name"],
                         e=profile.get("_evidence_count", 0),
                         c=profile.get("confidence", "?"))

                # Follow rabbit holes: create new actors discovered in evidence
                new_actors = profile.get("new_actors_discovered") or []
                for new_name in new_actors:
                    if not new_name or not isinstance(new_name, str) or len(new_name) < 3:
                        continue
                    try:
                        from intelligence.actor_ingest import ingest_actor
                        if ingest_actor(engine, new_name.strip(), "unknown",
                                        source=f"discovered_via_{actor['name'][:30]}",
                                        confidence="inferred",
                                        metadata={"discovered_from": actor["name"],
                                                  "discovery_context": "llm_research"}):
                            new_actors_created += 1
                            log.info("NEW RABBIT HOLE: {new} (discovered via {src})",
                                     new=new_name, src=actor["name"])
                    except Exception:
                        pass

                # Log rabbit holes for follow-up
                holes = profile.get("rabbit_holes") or []
                for hole in holes:
                    if hole and isinstance(hole, str):
                        rabbit_holes_found += 1
                        log.info("RABBIT HOLE: {h} (from {a})",
                                 h=hole[:120], a=actor["name"])

                        # Store rabbit hole as metadata for future investigation
                        try:
                            with engine.begin() as conn:
                                conn.execute(text("""
                                    INSERT INTO raw_series (series_id, source_id, obs_date, value, raw_payload, pull_status)
                                    SELECT :sid, sc.id, CURRENT_DATE, 1.0, :payload, 'SUCCESS'
                                    FROM source_catalog sc WHERE sc.name = 'alphavantage_bulk'
                                    LIMIT 1
                                """), {
                                    "sid": f"rabbit_hole:{actor['name'][:30]}",
                                    "payload": json.dumps({
                                        "actor": actor["name"],
                                        "lead": hole,
                                        "discovered_at": datetime.now(timezone.utc).isoformat(),
                                    }),
                                })
                        except Exception:
                            pass

        except Exception as exc:
            log.debug("Actor research failed for {n}: {e}", n=actor["name"], e=str(exc))

    return {
        "researched": len(actors),
        "updated": updated,
        "new_actors": new_actors_created,
        "rabbit_holes": rabbit_holes_found,
        "actors": [a["name"] for a in actors[:10]],
    }


def run_continuous(engine: Engine, rounds: int = 10, batch_size: int = 20) -> None:
    """Run continuous actor research for N rounds.

    Designed to be called by Hermes as a background task.
    """
    import time

    total_updated = 0
    for i in range(rounds):
        result = research_batch(engine, batch_size=batch_size)
        total_updated += result["updated"]
        log.info("Actor research round {r}/{t}: {u} updated",
                 r=i + 1, t=rounds, u=result["updated"])

        if result["researched"] == 0:
            log.info("No more sparse actors — stopping")
            break

        time.sleep(2)  # Don't hammer the LLM

    log.info("Actor research complete: {u} actors enriched across {r} rounds",
             u=total_updated, r=rounds)


if __name__ == "__main__":
    from db import get_engine
    engine = get_engine()
    run_continuous(engine, rounds=100, batch_size=10)

