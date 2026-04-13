#!/usr/bin/env python3
"""
Load hand-curated supply chain + capital flow seed data into griddb.

Reads:
  data/seed/supply_chain_seed.json
  data/seed/capital_flow_seed.json

Writes (idempotent, ON CONFLICT DO UPDATE):
  supply_chain_nodes
  supply_chain_edges
  capital_flows

Uses psycopg2 directly (no SQLAlchemy dependency on the loader path) because the
server venv has psycopg2 and we want a tight loop that works even if grid's
larger app environment is broken.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List

import psycopg2
from psycopg2.extras import execute_batch

REPO_ROOT = Path(__file__).resolve().parents[1]
SEED_DIR = REPO_ROOT / "data" / "seed"
SUPPLY_PATH = SEED_DIR / "supply_chain_seed.json"
CAPITAL_PATH = SEED_DIR / "capital_flow_seed.json"


def _connect():
    dsn = os.environ.get("GRID_DB_URL") or os.environ.get("DATABASE_URL")
    if dsn:
        return psycopg2.connect(dsn)
    return psycopg2.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=int(os.environ.get("PGPORT", "5432")),
        dbname=os.environ.get("PGDATABASE", "griddb"),
        user=os.environ.get("PGUSER", "grid"),
        password=os.environ.get("PGPASSWORD", ""),
    )


def _load_json(path: Path) -> Any:
    if not path.exists():
        sys.exit(f"seed file missing: {path}")
    with path.open() as fh:
        return json.load(fh)


def upsert_nodes(cur, nodes: Iterable[Dict[str, Any]]) -> int:
    sql = """
        INSERT INTO supply_chain_nodes
            (id, name, type, country, region, chokepoint_flag, notes)
        VALUES
            (%(id)s, %(name)s, %(type)s, %(country)s, %(region)s,
             %(chokepoint_flag)s, %(notes)s)
        ON CONFLICT (id) DO UPDATE SET
            name            = EXCLUDED.name,
            type            = EXCLUDED.type,
            country         = EXCLUDED.country,
            region          = EXCLUDED.region,
            chokepoint_flag = EXCLUDED.chokepoint_flag,
            notes           = EXCLUDED.notes
    """
    rows: List[Dict[str, Any]] = []
    for n in nodes:
        rows.append(
            {
                "id": n["id"],
                "name": n["name"],
                "type": n["type"],
                "country": n.get("country"),
                "region": n.get("region"),
                "chokepoint_flag": bool(n.get("chokepoint_flag", False)),
                "notes": n.get("notes"),
            }
        )
    execute_batch(cur, sql, rows, page_size=500)
    return len(rows)


def upsert_edges(cur, edges: Iterable[Dict[str, Any]]) -> int:
    sql = """
        INSERT INTO supply_chain_edges
            (upstream_id, downstream_id, relationship, tier, input_type,
             annual_usd, pct_upstream_revenue, pct_downstream_cogs,
             chokepoint_score, confidence, as_of, source)
        VALUES
            (%(upstream_id)s, %(downstream_id)s, %(relationship)s, %(tier)s,
             %(input_type)s, %(annual_usd)s, %(pct_upstream_revenue)s,
             %(pct_downstream_cogs)s, %(chokepoint_score)s, %(confidence)s,
             %(as_of)s, %(source)s)
        ON CONFLICT (upstream_id, downstream_id, relationship, as_of) DO UPDATE SET
            tier                 = EXCLUDED.tier,
            input_type           = EXCLUDED.input_type,
            annual_usd           = EXCLUDED.annual_usd,
            pct_upstream_revenue = EXCLUDED.pct_upstream_revenue,
            pct_downstream_cogs  = EXCLUDED.pct_downstream_cogs,
            chokepoint_score     = EXCLUDED.chokepoint_score,
            confidence           = EXCLUDED.confidence,
            source               = EXCLUDED.source
    """
    rows: List[Dict[str, Any]] = []
    for e in edges:
        rows.append(
            {
                "upstream_id": e["upstream_id"],
                "downstream_id": e["downstream_id"],
                "relationship": e["relationship"],
                "tier": int(e.get("tier", 1)),
                "input_type": e.get("input_type"),
                "annual_usd": e.get("annual_usd"),
                "pct_upstream_revenue": e.get("pct_upstream_revenue"),
                "pct_downstream_cogs": e.get("pct_downstream_cogs"),
                "chokepoint_score": e.get("chokepoint_score"),
                "confidence": e["confidence"],
                "as_of": e.get("as_of"),
                "source": e["source"],
            }
        )
    execute_batch(cur, sql, rows, page_size=500)
    return len(rows)


def upsert_capital_flows(cur, flows: Iterable[Dict[str, Any]]) -> int:
    # The unique constraint includes counterparty_id, which is nullable.
    # NULL != NULL in Postgres unique constraints, so rows with NULL
    # counterparty_id can technically duplicate. We dedupe in Python first.
    seen = set()
    rows: List[Dict[str, Any]] = []
    for f in flows:
        key = (
            f["actor_id"],
            f["fiscal_period"],
            f["period_type"],
            f["flow_type"],
            f.get("counterparty_id"),
            f.get("source_filing"),
        )
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "actor_id": f["actor_id"],
                "fiscal_period": f["fiscal_period"],
                "period_type": f["period_type"],
                "flow_type": f["flow_type"],
                "direction": f["direction"],
                "amount_usd": f["amount_usd"],
                "counterparty_id": f.get("counterparty_id"),
                "source_filing": f.get("source_filing"),
                "confidence": f["confidence"],
            }
        )

    # Delete-then-insert pattern handles NULL counterparty_id cleanly.
    delete_sql = """
        DELETE FROM capital_flows
        WHERE actor_id = %(actor_id)s
          AND fiscal_period = %(fiscal_period)s
          AND period_type = %(period_type)s
          AND flow_type = %(flow_type)s
          AND source_filing IS NOT DISTINCT FROM %(source_filing)s
          AND counterparty_id IS NOT DISTINCT FROM %(counterparty_id)s
    """
    insert_sql = """
        INSERT INTO capital_flows
            (actor_id, fiscal_period, period_type, flow_type, direction,
             amount_usd, counterparty_id, source_filing, confidence)
        VALUES
            (%(actor_id)s, %(fiscal_period)s, %(period_type)s, %(flow_type)s,
             %(direction)s, %(amount_usd)s, %(counterparty_id)s,
             %(source_filing)s, %(confidence)s)
    """
    execute_batch(cur, delete_sql, rows, page_size=500)
    execute_batch(cur, insert_sql, rows, page_size=500)
    return len(rows)


def main() -> int:
    supply = _load_json(SUPPLY_PATH)
    capital = _load_json(CAPITAL_PATH)

    nodes = supply.get("nodes", [])
    edges = supply.get("edges", [])
    flows = capital.get("flows", [])

    conn = _connect()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            n_nodes = upsert_nodes(cur, nodes)
            n_edges = upsert_edges(cur, edges)
            n_flows = upsert_capital_flows(cur, flows)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(f"supply_chain_nodes upserted: {n_nodes}")
    print(f"supply_chain_edges upserted: {n_edges}")
    print(f"capital_flows      upserted: {n_flows}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
