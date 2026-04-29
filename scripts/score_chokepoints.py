#!/usr/bin/env python3
"""
Score Supply Chain Chokepoints — runner script.

Scores every supply_chain_edges row with a NULL chokepoint_score and then flips
supply_chain_nodes.chokepoint_flag for nodes participating in any edge with a
score >= 0.7. Hand-curated values are preserved.

Usage:
    python3 scripts/score_chokepoints.py                   # defaults to griddb
    GRID_DB_URL=postgresql://... python3 scripts/score_chokepoints.py
"""

from __future__ import annotations

import os
import sys

# Allow running from the live server tree or a dev checkout.
for _candidate in ("/data/grid_v4/astrogrid_dedup", "/data/grid_v4/grid_repo"):
    if os.path.isdir(_candidate) and _candidate not in sys.path:
        sys.path.insert(0, _candidate)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loguru import logger as log
from sqlalchemy import create_engine, text

from intelligence.supply_chokepoints import (
    HIGH_SCORE_THRESHOLD,
    flag_chokepoint_nodes,
    score_all_edges,
)


DEFAULT_DB_URL = "postgresql://grid:gridmaster2026@localhost:5432/griddb"


def _db_url() -> str:
    return os.environ.get("GRID_DB_URL") or DEFAULT_DB_URL


def _counts(conn) -> dict[str, int]:
    row = conn.execute(
        text(
            "SELECT COUNT(*) AS edges, "
            "COUNT(chokepoint_score) AS scored, "
            "COUNT(*) - COUNT(chokepoint_score) AS unscored "
            "FROM supply_chain_edges"
        )
    ).fetchone()
    node_row = conn.execute(
        text(
            "SELECT COUNT(*) FILTER (WHERE chokepoint_flag) AS flagged, "
            "COUNT(*) AS total FROM supply_chain_nodes"
        )
    ).fetchone()
    return {
        "edges": int(row[0] or 0),
        "scored": int(row[1] or 0),
        "unscored": int(row[2] or 0),
        "nodes_flagged": int(node_row[0] or 0),
        "nodes_total": int(node_row[1] or 0),
    }


def _distribution(conn) -> dict[str, int]:
    row = conn.execute(
        text(
            """
            SELECT
              COUNT(*) FILTER (WHERE chokepoint_score >= 0.7) AS high,
              COUNT(*) FILTER (WHERE chokepoint_score >= 0.5 AND chokepoint_score < 0.7) AS mid,
              COUNT(*) FILTER (WHERE chokepoint_score > 0 AND chokepoint_score < 0.3) AS low,
              COUNT(*) FILTER (WHERE chokepoint_score IS NOT NULL) AS any_score
            FROM supply_chain_edges
            """
        )
    ).fetchone()
    return {
        "ge_0.7": int(row[0] or 0),
        "0.5_to_0.7": int(row[1] or 0),
        "lt_0.3": int(row[2] or 0),
        "scored_total": int(row[3] or 0),
    }


def _top_edges(conn, n: int = 10) -> list[tuple]:
    rows = conn.execute(
        text(
            """
            SELECT upstream_id, downstream_id, input_type, chokepoint_score
            FROM supply_chain_edges
            WHERE chokepoint_score IS NOT NULL
            ORDER BY chokepoint_score DESC, id ASC
            LIMIT :n
            """
        ),
        {"n": n},
    ).fetchall()
    return [(r[0], r[1], r[2], float(r[3])) for r in rows]


def main() -> int:
    db_url = _db_url()
    log.info("supply_chokepoints runner: db={u}", u=db_url.split("@")[-1])
    engine = create_engine(db_url, pool_pre_ping=True)

    with engine.connect() as conn:
        before = _counts(conn)
    log.info("before: {b}", b=before)

    score_stats = score_all_edges(engine)
    flag_stats = flag_chokepoint_nodes(engine, threshold=HIGH_SCORE_THRESHOLD)

    with engine.connect() as conn:
        after = _counts(conn)
        dist = _distribution(conn)
        top = _top_edges(conn, 10)

    print("\n=== SUPPLY CHOKEPOINT SCORING ===")
    print(f"Before : edges={before['edges']}  scored={before['scored']}  "
          f"unscored={before['unscored']}  nodes_flagged={before['nodes_flagged']}")
    print(f"Score  : {score_stats}")
    print(f"After  : edges={after['edges']}  scored={after['scored']}  "
          f"unscored={after['unscored']}  nodes_flagged={after['nodes_flagged']}")
    print(f"Flags  : {flag_stats}")
    print(f"Distrib: >=0.7={dist['ge_0.7']}  0.5-0.7={dist['0.5_to_0.7']}  "
          f"<0.3={dist['lt_0.3']}  scored_total={dist['scored_total']}")
    print("\nTop 10 chokepoints:")
    for i, (u, d, t, s) in enumerate(top, 1):
        print(f"  {i:2d}. {u} -> {d}  [{t or '-'}]  score={s:.3f}")
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
