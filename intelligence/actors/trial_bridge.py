"""
GRID Intelligence — Trial Sponsor → Actor Network bridge.

Reads trial_cache and trial_signals from griddb, extracts unique sponsors,
upserts them as actors, and builds connections:

- sponsor ↔ sponsor (same indication = therapeutic competition)
- sponsor ↔ sponsor (same trial = co-sponsorship)
- sponsor ↔ existing actor (lobbying firms, politicians on health committees)
- sponsor → wealth_flows (trial funding as capital deployment signal)

Run standalone:
    python3 -m intelligence.actors.trial_bridge

Or called from trial_ingestor.py after each ingestion run.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from typing import Any

import psycopg2
import psycopg2.extras

log = logging.getLogger("grid.intelligence.trial_bridge")


# Health-sector actors that pharma/biotech sponsors naturally connect to
HEALTH_COMMITTEE_ACTORS = [
    "sen_bernie_sanders",    # Senate HELP committee
    "sen_bill_cassidy",      # Senate HELP committee
    "rep_frank_pallone",     # House Energy & Commerce
    "rep_cathy_mcmorris",    # House Energy & Commerce
]

# Lobbying firms known to represent pharma
PHARMA_LOBBY_ACTORS = [
    "lobby_invariant",
    "lobby_akin_gump",
    "lobby_covington",
    "lobby_hogan_lovells",
]


def sync_trial_sponsors_to_actors(conn) -> dict[str, Any]:
    """
    Main entry point. Reads trial data, creates/updates actors, builds connections.

    Returns summary dict with counts.
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # 1. Extract unique sponsors from trial_cache
    sponsors = _extract_sponsors(cur)
    log.info(f"Found {len(sponsors)} unique trial sponsors")

    # 2. Upsert each sponsor as an actor
    upserted = _upsert_sponsor_actors(conn, sponsors)
    log.info(f"Upserted {upserted} sponsor actors")

    # 3. Build connections between sponsors (shared indications, co-sponsorship)
    connections = _build_sponsor_connections(cur, sponsors)
    _write_connections(conn, connections)
    log.info(f"Built {len(connections)} sponsor connections")

    # 4. Link sponsors to existing health-sector actors
    health_links = _link_to_health_actors(conn, sponsors)
    log.info(f"Linked {health_links} sponsors to health-sector actors")

    # 5. Record trial funding as wealth flows
    flows = _record_trial_wealth_flows(conn, cur)
    log.info(f"Recorded {flows} trial wealth flows")

    cur.close()
    return {
        "sponsors": len(sponsors),
        "actors_upserted": upserted,
        "connections": len(connections),
        "health_links": health_links,
        "wealth_flows": flows,
    }


def _extract_sponsors(cur) -> dict[str, dict]:
    """
    Extract unique sponsors from trial_cache with trial counts and indications.
    Returns: {sponsor_name: {trial_count, indications, phases, class, nct_ids}}
    """
    sponsors = defaultdict(lambda: {
        "trial_count": 0,
        "indications": set(),
        "phases": set(),
        "class": "",
        "nct_ids": [],
    })

    cur.execute("SELECT nct_id, raw_json FROM trial_cache WHERE expires_at > NOW()")
    for row in cur:
        raw = row["raw_json"] if isinstance(row["raw_json"], dict) else json.loads(row["raw_json"])
        proto = raw.get("protocolSection", {})
        sponsor_mod = proto.get("sponsorCollaboratorsModule", {})
        lead = sponsor_mod.get("leadSponsor", {})
        name = lead.get("name", "").strip()
        if not name:
            continue

        sponsors[name]["trial_count"] += 1
        sponsors[name]["class"] = lead.get("class", "")
        sponsors[name]["nct_ids"].append(row["nct_id"])

        # Extract conditions
        conditions = proto.get("conditionsModule", {}).get("conditions", [])
        for c in conditions:
            sponsors[name]["indications"].add(c.lower())

        # Extract phases
        phases = proto.get("designModule", {}).get("phases", [])
        for p in phases:
            sponsors[name]["phases"].add(p)

        # Also grab collaborators for co-sponsorship connections
        collabs = sponsor_mod.get("collaborators", [])
        for c in collabs:
            cname = c.get("name", "").strip()
            if cname:
                sponsors[cname]["trial_count"] += 1
                sponsors[cname]["class"] = c.get("class", "")
                sponsors[cname]["nct_ids"].append(row["nct_id"])

    # Convert sets to lists for JSON serialization
    for s in sponsors.values():
        s["indications"] = list(s["indications"])
        s["phases"] = list(s["phases"])

    return dict(sponsors)


def _sponsor_actor_id(name: str) -> str:
    """Generate a deterministic actor ID from sponsor name."""
    clean = name.lower().strip()
    for suffix in [", inc.", ", inc", " inc.", " inc", ", ltd.", ", ltd",
                   " ltd.", " ltd", " llc", " plc", " corp.", " corp",
                   " co.", " co", " s.a.", " ag", " se", " nv",
                   " gmbh", " pty", " srl", " b.v.", " n.v."]:
        if clean.endswith(suffix):
            clean = clean[: -len(suffix)].strip()
            break
    slug = clean.replace(" ", "_").replace(",", "").replace(".", "")[:40]
    return f"trial_{slug}"


def _determine_tier(sponsor_data: dict) -> str:
    """Determine actor tier based on trial count and sponsor class."""
    if sponsor_data["trial_count"] >= 50:
        return "institutional"
    if sponsor_data["trial_count"] >= 10:
        return "institutional"
    return "individual"  # small academic labs etc.


def _determine_category(sponsor_data: dict) -> str:
    cls = sponsor_data["class"].upper()
    if cls == "INDUSTRY":
        return "pharmaceutical_sponsor"
    if cls in ("NIH", "FED"):
        return "government_research"
    if cls in ("NETWORK", "OTHER"):
        return "academic_research"
    return "clinical_sponsor"


def _upsert_sponsor_actors(conn, sponsors: dict[str, dict]) -> int:
    """Upsert sponsor actors into the actors table."""
    cur = conn.cursor()
    count = 0

    for name, data in sponsors.items():
        actor_id = _sponsor_actor_id(name)
        tier = _determine_tier(data)
        category = _determine_category(data)

        phase_str = ", ".join(sorted(data["phases"]))
        indication_str = ", ".join(sorted(data["indications"])[:5])
        title = (
            f"{data['trial_count']} active trials | "
            f"{phase_str} | {indication_str[:80]}"
        )

        metadata = json.dumps({
            "trial_count": data["trial_count"],
            "phases": data["phases"],
            "top_indications": data["indications"][:10],
            "sponsor_class": data["class"],
            "source": "clinicaltrials.gov",
        })

        try:
            cur.execute("""
                INSERT INTO actors (
                    id, name, tier, category, title,
                    influence_score, trust_score, motivation_model,
                    data_sources, credibility, metadata, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, NOW()
                )
                ON CONFLICT (id) DO UPDATE SET
                    title = EXCLUDED.title,
                    influence_score = GREATEST(actors.influence_score, EXCLUDED.influence_score),
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW()
            """, (
                actor_id, name, tier, category, title,
                min(0.95, 0.4 + data["trial_count"] * 0.005),  # influence scales with trial count
                0.9,  # CT.gov data is high trust
                "pipeline_velocity" if category == "pharmaceutical_sponsor" else "research_mandate",
                json.dumps(["clinicaltrials_gov"]),
                "hard_data",
                metadata,
            ))
            count += 1
        except Exception as e:
            log.warning(f"Failed to upsert actor {actor_id}: {e}")
            conn.rollback()

    conn.commit()
    cur.close()
    return count


def _build_sponsor_connections(cur, sponsors: dict[str, dict]) -> list[dict]:
    """
    Build connections between sponsors based on:
    - Shared indications (therapeutic_competition)
    - Co-sponsored trials (clinical_partnership)
    """
    connections = []

    # Build indication → sponsors index
    indication_to_sponsors = defaultdict(set)
    for name, data in sponsors.items():
        for ind in data["indications"]:
            indication_to_sponsors[ind].add(name)

    # Build nct_id → sponsors index for co-sponsorship
    nct_to_sponsors = defaultdict(set)
    for name, data in sponsors.items():
        for nct in data["nct_ids"]:
            nct_to_sponsors[nct].add(name)

    seen = set()

    # Therapeutic competition: shared indications
    for indication, sponsor_names in indication_to_sponsors.items():
        names = sorted(sponsor_names)
        for i, a in enumerate(names):
            for b in names[i + 1:]:
                key = (a, b)
                if key in seen:
                    continue
                seen.add(key)
                connections.append({
                    "from": _sponsor_actor_id(a),
                    "to": _sponsor_actor_id(b),
                    "type": "therapeutic_competition",
                    "strength": 0.6,
                    "evidence": f"shared indication: {indication}",
                })

    # Co-sponsorship: same trial
    for nct_id, sponsor_names in nct_to_sponsors.items():
        if len(sponsor_names) < 2:
            continue
        names = sorted(sponsor_names)
        for i, a in enumerate(names):
            for b in names[i + 1:]:
                key = (a, b)
                if key not in seen:
                    seen.add(key)
                    connections.append({
                        "from": _sponsor_actor_id(a),
                        "to": _sponsor_actor_id(b),
                        "type": "clinical_partnership",
                        "strength": 0.8,
                        "evidence": f"co-sponsor: {nct_id}",
                    })

    return connections


def _write_connections(conn, connections: list[dict]) -> None:
    """Write connections as JSONB updates on the actors table."""
    if not connections:
        return

    # Group connections by actor
    actor_connections = defaultdict(list)
    for c in connections:
        actor_connections[c["from"]].append({
            "actor": c["to"],
            "type": c["type"],
            "strength": c["strength"],
        })
        actor_connections[c["to"]].append({
            "actor": c["from"],
            "type": c["type"],
            "strength": c["strength"],
        })

    cur = conn.cursor()
    for actor_id, conns in actor_connections.items():
        try:
            cur.execute("""
                UPDATE actors
                SET connections = %s::jsonb, updated_at = NOW()
                WHERE id = %s
            """, (json.dumps(conns), actor_id))
        except Exception as e:
            log.warning(f"Failed to write connections for {actor_id}: {e}")
            conn.rollback()

    conn.commit()
    cur.close()


def _link_to_health_actors(conn, sponsors: dict[str, dict]) -> int:
    """
    Link INDUSTRY sponsors to existing health-committee politicians
    and pharma lobbying firms in the actor network.
    """
    cur = conn.cursor()
    count = 0

    # Check which health actors actually exist in the DB
    existing_health = []
    for aid in HEALTH_COMMITTEE_ACTORS + PHARMA_LOBBY_ACTORS:
        cur.execute("SELECT id FROM actors WHERE id = %s", (aid,))
        if cur.fetchone():
            existing_health.append(aid)

    if not existing_health:
        cur.close()
        return 0

    # For each INDUSTRY sponsor with 5+ trials, link to health actors
    for name, data in sponsors.items():
        if data["class"] != "INDUSTRY" or data["trial_count"] < 5:
            continue
        actor_id = _sponsor_actor_id(name)

        # Get existing connections
        cur.execute("SELECT connections FROM actors WHERE id = %s", (actor_id,))
        row = cur.fetchone()
        if not row:
            continue

        existing = json.loads(row[0]) if isinstance(row[0], str) else (row[0] or [])
        existing_targets = {c.get("actor") for c in existing}

        new_conns = list(existing)
        for health_id in existing_health:
            if health_id not in existing_targets:
                rel_type = "lobbying_target" if health_id.startswith("lobby_") else "regulatory_influence"
                new_conns.append({
                    "actor": health_id,
                    "type": rel_type,
                    "strength": 0.4,
                })
                count += 1

        cur.execute(
            "UPDATE actors SET connections = %s::jsonb, updated_at = NOW() WHERE id = %s",
            (json.dumps(new_conns), actor_id),
        )

    conn.commit()
    cur.close()
    return count


def _record_trial_wealth_flows(conn, cur) -> int:
    """
    Record trial funding as wealth flows:
    sponsor → trial as capital deployment signal.
    Uses enrollment count as rough proxy for trial cost.
    """
    flow_cur = conn.cursor()
    count = 0

    cur.execute("""
        SELECT nct_id, raw_json FROM trial_cache
        WHERE expires_at > NOW()
    """)

    for row in cur:
        raw = row["raw_json"] if isinstance(row["raw_json"], dict) else json.loads(row["raw_json"])
        proto = raw.get("protocolSection", {})
        sponsor = proto.get("sponsorCollaboratorsModule", {}).get("leadSponsor", {})
        name = sponsor.get("name", "").strip()
        if not name or sponsor.get("class", "") != "INDUSTRY":
            continue

        enrollment = proto.get("designModule", {}).get("enrollmentInfo", {}).get("count", 0)
        if not enrollment or enrollment < 10:
            continue

        actor_id = _sponsor_actor_id(name)
        # Rough cost estimate: ~$50K per enrolled patient for Phase 2/3
        estimated_cost = enrollment * 50_000

        try:
            flow_cur.execute("""
                INSERT INTO wealth_flows (
                    from_actor, to_entity, amount_estimate,
                    confidence, evidence, implication
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                actor_id,
                f"trial_{row['nct_id']}",
                estimated_cost,
                "estimated",
                json.dumps([f"CT.gov enrollment: {enrollment} patients"]),
                f"Clinical trial capital deployment — {name} investing ~${estimated_cost/1e6:.1f}M",
            ))
            count += 1
        except Exception:
            conn.rollback()

    conn.commit()
    flow_cur.close()
    return count


if __name__ == "__main__":
    import os
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 5432)),
        dbname=os.getenv("DB_NAME", "griddb"),
        user=os.getenv("DB_USER", "grid"),
        password=os.getenv("DB_PASSWORD", ""),
    )

    result = sync_trial_sponsors_to_actors(conn)
    conn.close()

    print("\nTrial → Actor Bridge complete:")
    for k, v in result.items():
        print(f"  {k}: {v}")
