"""Fast AGE sync — individual MERGE per actor, then connections."""

from __future__ import annotations

import sys
import time

sys.path.insert(0, ".")
from loguru import logger as log
from sqlalchemy import text
from db import get_engine


def _esc(val):
    """Escape for Cypher string literal."""
    if val is None:
        return ""
    return str(val).replace("\\", "\\\\").replace("'", "\\'")


def sync():
    engine = get_engine()

    # ── Actors ──────────────────────────────────────────────────
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT id, name, tier, category, COALESCE(title, '') as title, "
            "COALESCE(influence_score, 0) as inf, "
            "COALESCE(trust_score, 0.5) as trust, "
            "COALESCE(credibility, 'inferred') as cred "
            "FROM actors ORDER BY influence_score DESC NULLS LAST LIMIT 10000"
        )).fetchall()

    log.info("Syncing {n} actors to AGE...", n=len(rows))
    t0 = time.time()
    synced = 0
    errors = 0

    for row in rows:
        r = dict(row._mapping)
        aid = _esc(r["id"])
        name = _esc(r["name"])
        tier = _esc(r["tier"] or "unknown")
        cat = _esc(r["category"] or "unknown")
        title = _esc(r["title"])
        inf = float(r["inf"] or 0)
        trust = float(r["trust"] or 0.5)
        cred = _esc(r["cred"])

        cypher = (
            f"MERGE (a:Actor {{actor_id: '{aid}'}}) "
            f"SET a.name = '{name}', a.tier = '{tier}', a.category = '{cat}', "
            f"a.title = '{title}', a.influence_score = {inf}, "
            f"a.trust_score = {trust}, a.credibility = '{cred}' "
            f"RETURN a"
        )
        sql = f"SELECT * FROM cypher('grid_graph', $$ {cypher} $$) AS (result agtype)"

        try:
            with engine.begin() as conn:
                conn.execute(text("SET search_path = ag_catalog, public"))
                conn.execute(text(sql))
            synced += 1
        except Exception as exc:
            errors += 1
            if errors <= 5:
                log.warning("Actor {id} failed: {e}", id=r["id"], e=str(exc)[:100])

        if synced % 1000 == 0 and synced > 0:
            elapsed = time.time() - t0
            log.info("  Actors: {n}/{t} ({r:.0f}/s, {e} errors)",
                     n=synced, t=len(rows), r=synced / elapsed, e=errors)

    elapsed = time.time() - t0
    log.info("Actors done: {n} synced in {t:.1f}s ({e} errors)",
             n=synced, t=elapsed, e=errors)

    # ── Connections ─────────────────────────────────────────────
    with engine.connect() as conn:
        conn_rows = conn.execute(text(
            "SELECT actor_a, actor_b, relationship, COALESCE(strength, 0.5) as strength "
            "FROM actor_connections LIMIT 50000"
        )).fetchall()

    log.info("Syncing {n} connections to AGE...", n=len(conn_rows))
    t1 = time.time()
    c_synced = 0
    c_errors = 0

    for row in conn_rows:
        r = dict(row._mapping)
        a = _esc(r["actor_a"])
        b = _esc(r["actor_b"])
        rel = _esc(r["relationship"] or "CONNECTS")
        strength = float(r["strength"] or 0.5)

        cypher = (
            f"MATCH (a:Actor {{actor_id: '{a}'}}), (b:Actor {{actor_id: '{b}'}}) "
            f"MERGE (a)-[r:CONNECTS {{relationship: '{rel}', strength: {strength}}}]->(b) "
            f"RETURN r"
        )
        sql = f"SELECT * FROM cypher('grid_graph', $$ {cypher} $$) AS (result agtype)"

        try:
            with engine.begin() as conn:
                conn.execute(text("SET search_path = ag_catalog, public"))
                conn.execute(text(sql))
            c_synced += 1
        except Exception as exc:
            c_errors += 1
            if c_errors <= 5:
                log.warning("Connection {a}->{b} failed: {e}",
                            a=r["actor_a"], b=r["actor_b"], e=str(exc)[:100])

        if c_synced % 5000 == 0 and c_synced > 0:
            elapsed = time.time() - t1
            log.info("  Connections: {n}/{t} ({r:.0f}/s, {e} errors)",
                     n=c_synced, t=len(conn_rows), r=c_synced / elapsed, e=c_errors)

    elapsed = time.time() - t1
    log.info("Connections done: {n} synced in {t:.1f}s ({e} errors)",
             n=c_synced, t=elapsed, e=c_errors)

    # ── Verify ──────────────────────────────────────────────────
    with engine.connect() as conn:
        conn.execute(text("SET search_path = ag_catalog, public"))
        actors = conn.execute(text(
            "SELECT * FROM cypher('grid_graph', $$ MATCH (a:Actor) RETURN count(a) $$) AS (cnt agtype)"
        )).fetchone()
        edges = conn.execute(text(
            "SELECT * FROM cypher('grid_graph', $$ MATCH ()-[r:CONNECTS]->() RETURN count(r) $$) AS (cnt agtype)"
        )).fetchone()
    log.info("AGE graph: {a} actors, {e} connections", a=actors[0], e=edges[0])


if __name__ == "__main__":
    sync()
