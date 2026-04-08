"""Sync actors and connections from relational tables into Apache AGE graph.

Usage:
    python scripts/sync_actors_to_age.py [--limit N] [--batch-size N]

This creates Actor vertices from the `actors` table and CONNECTS edges
from the `actor_connections` table in the grid_graph.

Safe to re-run — uses MERGE (upsert) semantics.
"""

from __future__ import annotations

import argparse
import json
import sys
import time

from loguru import logger as log
from sqlalchemy import text

sys.path.insert(0, ".")
from db import get_engine


def _escape(val: str) -> str:
    """Escape single quotes for Cypher string literals."""
    if val is None:
        return ""
    return str(val).replace("\\", "\\\\").replace("'", "\\'")


def sync_actors(engine, limit: int | None = None, batch_size: int = 500) -> int:
    """Load actors into AGE Actor vertices."""
    log.info("Syncing actors to AGE graph...")

    with engine.connect() as conn:
        q = "SELECT id, name, tier, category, title, influence_score, trust_score, motivation_model, credibility FROM actors ORDER BY influence_score DESC"
        if limit:
            q += f" LIMIT {limit}"
        rows = conn.execute(text(q)).fetchall()

    total = len(rows)
    log.info("Found {n} actors to sync", n=total)
    synced = 0

    for i in range(0, total, batch_size):
        batch = rows[i : i + batch_size]
        cypher_parts = []
        for row in batch:
            r = dict(row._mapping)
            props = {
                "actor_id": r["id"],
                "name": _escape(r["name"]),
                "tier": r["tier"] or "unknown",
                "category": r["category"] or "unknown",
                "title": _escape(r.get("title") or ""),
                "influence_score": float(r.get("influence_score") or 0),
                "trust_score": float(r.get("trust_score") or 0.5),
                "motivation_model": r.get("motivation_model") or "unknown",
                "credibility": r.get("credibility") or "inferred",
            }
            cypher_parts.append(
                f"MERGE (a:Actor {{actor_id: '{props['actor_id']}'}}) "
                f"SET a.name = '{props['name']}', "
                f"a.tier = '{props['tier']}', "
                f"a.category = '{props['category']}', "
                f"a.title = '{props['title']}', "
                f"a.influence_score = {props['influence_score']}, "
                f"a.trust_score = {props['trust_score']}, "
                f"a.credibility = '{props['credibility']}'"
            )

        # Execute batch as single Cypher query
        cypher = "\n".join(cypher_parts)
        cypher_sql = f"""
            SELECT * FROM cypher('grid_graph', $$
                {cypher}
            $$) AS (result agtype)
        """

        try:
            with engine.begin() as conn:
                conn.execute(text("SET search_path = ag_catalog, public"))
                conn.execute(text(cypher_sql))
            synced += len(batch)
            if synced % 5000 == 0 or synced == total:
                log.info("Actors synced: {n}/{total}", n=synced, total=total)
        except Exception as exc:
            log.warning("Batch failed at offset {i}: {e}", i=i, e=str(exc))
            # Try individual inserts for failed batch
            for row in batch:
                try:
                    r = dict(row._mapping)
                    name = _escape(r["name"])
                    single_sql = f"""
                        SELECT * FROM cypher('grid_graph', $$
                            MERGE (a:Actor {{actor_id: '{r["id"]}'}})
                            SET a.name = '{name}',
                                a.tier = '{r.get("tier", "unknown")}',
                                a.category = '{r.get("category", "unknown")}',
                                a.influence_score = {float(r.get("influence_score") or 0)}
                        $$) AS (result agtype)
                    """
                    with engine.begin() as conn:
                        conn.execute(text("LOAD 'age'"))
                        conn.execute(text("SET search_path = ag_catalog, public"))
                        conn.execute(text(single_sql))
                    synced += 1
                except Exception:
                    pass

    log.info("Actor sync complete: {n} synced", n=synced)
    return synced


def sync_connections(engine, limit: int | None = None, batch_size: int = 200) -> int:
    """Load connections into AGE edges."""
    log.info("Syncing connections to AGE graph...")

    with engine.connect() as conn:
        q = "SELECT actor_a, actor_b, relationship, strength FROM actor_connections ORDER BY id"
        if limit:
            q += f" LIMIT {limit}"
        rows = conn.execute(text(q)).fetchall()

    total = len(rows)
    log.info("Found {n} connections to sync", n=total)
    synced = 0

    for i in range(0, total, batch_size):
        batch = rows[i : i + batch_size]
        cypher_parts = []
        for row in batch:
            r = dict(row._mapping)
            rel = _escape(r["relationship"] or "CONNECTS")
            strength = float(r.get("strength") or 0.5)
            cypher_parts.append(
                f"MATCH (a:Actor {{actor_id: '{r['actor_a']}'}}), (b:Actor {{actor_id: '{r['actor_b']}'}}) "
                f"MERGE (a)-[r:CONNECTS {{relationship: '{rel}', strength: {strength}}}]->(b)"
            )

        cypher = "\n".join(cypher_parts)
        cypher_sql = f"""
            SELECT * FROM cypher('grid_graph', $$
                {cypher}
            $$) AS (result agtype)
        """

        try:
            with engine.begin() as conn:
                conn.execute(text("SET search_path = ag_catalog, public"))
                conn.execute(text(cypher_sql))
            synced += len(batch)
            if synced % 10000 == 0 or synced == total:
                log.info("Connections synced: {n}/{total}", n=synced, total=total)
        except Exception as exc:
            log.warning("Connection batch failed at {i}: {e}", i=i, e=str(exc))

    log.info("Connection sync complete: {n} synced", n=synced)
    return synced


def main():
    parser = argparse.ArgumentParser(description="Sync actors to AGE graph")
    parser.add_argument("--limit", type=int, default=None, help="Limit actors to sync")
    parser.add_argument("--batch-size", type=int, default=500, help="Batch size")
    parser.add_argument("--connections-limit", type=int, default=None, help="Limit connections")
    args = parser.parse_args()

    engine = get_engine()

    t0 = time.time()
    actors = sync_actors(engine, limit=args.limit, batch_size=args.batch_size)
    connections = sync_connections(engine, limit=args.connections_limit, batch_size=200)
    elapsed = time.time() - t0

    log.info(
        "AGE sync complete: {a} actors, {c} connections in {t:.1f}s",
        a=actors, c=connections, t=elapsed,
    )


if __name__ == "__main__":
    main()
