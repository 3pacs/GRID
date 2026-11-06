"""Versioned actor-universe graph analytics tables + the indexes the full-scale API reads.

Revision ID: actor_universe_20261106
Revises: api_precomputed_results_20261106

Why
---
The actor network was built in Python inside a web request
(``intelligence/actors/graph.py::build_actor_graph``) and froze grid-api on
2026-11-06; #995 moved it to a background thread capped at ~39K curated
actors. The universe is ~3.49M actors and ~11M edges. The offline job
``intelligence/actors/universe.py`` now computes PageRank, communities,
components, k-core, sampled betweenness and sectors over all of it, and the
API only reads the result. ``actor_analytics`` (one unversioned row per
actor, last written 2026-04-08) is left untouched for its existing readers.

Tables (additive, versioned by ``graph_version``)
------------------------------------------------
* ``actor_graph_runs`` -- one row per job run: status
  (running/complete/failed), counts, runtime, peak RSS, stats JSON. Readers
  use the newest ``complete`` row.
* ``actor_graph_metrics`` -- one row per (version, actor): category group,
  sector (+ basis), pagerank, importance, degree, weighted degree, sampled
  betweenness, coreness, community, component, rank within group, overview
  pool flag. Indexed for the overview reads (pool members by group/sector,
  and by community, ordered by importance).
* ``actor_graph_communities`` -- per (version, community) summary.
* ``actor_graph_edges`` -- edges among the overview pool (both endpoints in
  the pool, capped per node), so the overview graph is two indexed reads.

The job writes a version's rows and flips its run to ``complete`` in one
transaction and keeps the two newest complete versions.

Indexes on existing tables
--------------------------
* ``idx_wealth_flows_to_entity`` on ``wealth_flows(to_entity)`` (~1.04M
  rows): the ego network and the ticker context read inbound flows
  (insider -> ``corp_<TICKER>``). Built ``CONCURRENTLY`` outside the
  migration transaction when the table is large; a failed concurrent build's
  INVALID leftover is dropped and rebuilt. ``CONCURRENTLY`` waits for every
  older snapshot in the database (see ``snapshot_actor_index_20260912``), so
  on production this step lasts as long as the longest open transaction --
  no write lock is taken. ``statement_timeout`` is lifted for it.
* ``idx_actors_name_trgm`` (GIN trigram on ``actors.name``) already exists on
  griddb (2026-11-06 catalog read). It is created here only where it is
  missing *and* ``actors`` is small (fresh/CI databases); on a large table
  without it the migration logs a warning instead -- that build needs a
  maintenance window (``CREATE INDEX CONCURRENTLY idx_actors_name_trgm ON
  actors USING gin (name gin_trgm_ops)``, ~4.4 GB heap).

GRANT footer per ``migrations/_TEMPLATE.sql``: ``grid`` gets full DML on the
four new tables (the job deletes superseded versions).
"""

from __future__ import annotations

import logging

from alembic import op
from sqlalchemy import text

revision = "actor_universe_20261106"
down_revision = "api_precomputed_results_20261106"
branch_labels = None
depends_on = None

_log = logging.getLogger("alembic.runtime.migration")

# Above this many rows an index on an existing table is built CONCURRENTLY
# (outside the transaction) instead of inline.
INLINE_INDEX_MAX_ROWS = 200_000

NEW_TABLES = ("actor_graph_runs", "actor_graph_metrics", "actor_graph_communities", "actor_graph_edges")

_DDL = [
    """
    CREATE TABLE IF NOT EXISTS actor_graph_runs (
        graph_version         TEXT PRIMARY KEY,
        status                TEXT NOT NULL CHECK (status IN ('running', 'complete', 'failed')),
        started_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        computed_at           TIMESTAMPTZ,
        host                  TEXT,
        node_count            BIGINT,
        edge_count            BIGINT,
        actor_count           BIGINT,
        external_node_count   BIGINT,
        community_count       BIGINT,
        component_count       BIGINT,
        giant_component_size  BIGINT,
        runtime_s             DOUBLE PRECISION,
        peak_rss_mb           DOUBLE PRECISION,
        params                JSONB NOT NULL DEFAULT '{}'::jsonb,
        stats                 JSONB NOT NULL DEFAULT '{}'::jsonb,
        error                 TEXT
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_actor_graph_runs_complete
        ON actor_graph_runs (computed_at DESC) WHERE status = 'complete'
    """,
    """
    CREATE TABLE IF NOT EXISTS actor_graph_metrics (
        graph_version     TEXT NOT NULL,
        actor_id          TEXT NOT NULL,
        category          TEXT NOT NULL,
        category_group    TEXT NOT NULL,
        sector            TEXT NOT NULL,
        sector_basis      TEXT NOT NULL,
        pagerank          DOUBLE PRECISION NOT NULL DEFAULT 0,
        importance        DOUBLE PRECISION NOT NULL DEFAULT 0,
        degree            INTEGER NOT NULL DEFAULT 0,
        weighted_degree   DOUBLE PRECISION NOT NULL DEFAULT 0,
        betweenness       DOUBLE PRECISION NOT NULL DEFAULT 0,
        coreness          INTEGER NOT NULL DEFAULT 0,
        community_id      INTEGER,
        component_id      INTEGER,
        group_rank        INTEGER,
        in_overview_pool  BOOLEAN NOT NULL DEFAULT FALSE,
        PRIMARY KEY (graph_version, actor_id)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_actor_graph_metrics_group_importance
        ON actor_graph_metrics (graph_version, category_group, importance DESC)
        WHERE in_overview_pool
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_actor_graph_metrics_sector_importance
        ON actor_graph_metrics (graph_version, sector, category_group, importance DESC)
        WHERE in_overview_pool
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_actor_graph_metrics_community_importance
        ON actor_graph_metrics (graph_version, community_id, importance DESC)
    """,
    """
    CREATE TABLE IF NOT EXISTS actor_graph_communities (
        graph_version      TEXT NOT NULL,
        community_id       INTEGER NOT NULL,
        size               INTEGER NOT NULL,
        actor_count        INTEGER NOT NULL,
        internal_edges     BIGINT NOT NULL DEFAULT 0,
        top_actor_ids      TEXT[] NOT NULL DEFAULT '{}',
        top_sectors        JSONB NOT NULL DEFAULT '[]'::jsonb,
        top_categories     JSONB NOT NULL DEFAULT '[]'::jsonb,
        dominant_sector    TEXT,
        dominant_category  TEXT,
        max_importance     DOUBLE PRECISION NOT NULL DEFAULT 0,
        PRIMARY KEY (graph_version, community_id)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_actor_graph_communities_size
        ON actor_graph_communities (graph_version, size DESC)
    """,
    """
    CREATE TABLE IF NOT EXISTS actor_graph_edges (
        graph_version  TEXT NOT NULL,
        src            TEXT NOT NULL,
        dst            TEXT NOT NULL,
        weight         DOUBLE PRECISION NOT NULL DEFAULT 0.5,
        relationship   TEXT
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_actor_graph_edges_src
        ON actor_graph_edges (graph_version, src)
    """,
]


def _estimated_rows(conn, table: str) -> int:
    row = conn.execute(
        text("SELECT reltuples::bigint FROM pg_class WHERE oid = to_regclass(:t)"), {"t": table}
    ).fetchone()
    if row is None:
        return -1  # table absent
    return max(int(row[0] or 0), 0)


def _index_state(conn, name: str) -> str:
    """'absent', 'valid' or 'invalid' for an index name in the search path."""
    row = conn.execute(
        text(
            "SELECT i.indisvalid FROM pg_index i "
            "WHERE i.indexrelid = to_regclass(:n)"
        ),
        {"n": name},
    ).fetchone()
    if row is None:
        return "absent"
    return "valid" if row[0] else "invalid"


def _ensure_index(name: str, table: str, definition: str, *, allow_concurrent: bool) -> None:
    """Create ``name`` inline on small tables, CONCURRENTLY on large ones.

    ``definition`` is the ``ON table USING ... (...)`` tail (a module
    constant, never user input).
    """
    conn = op.get_bind()
    rows = _estimated_rows(conn, table)
    if rows < 0:
        return
    state = _index_state(conn, name)
    if state == "valid":
        return
    if rows <= INLINE_INDEX_MAX_ROWS:
        if state == "invalid":
            conn.execute(text(f"DROP INDEX IF EXISTS {name}"))
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS {name} {definition}"))
        return
    if not allow_concurrent:
        _log.warning(
            "%s is missing on %s (~%s rows): not built inline; build it in a "
            "maintenance window: CREATE INDEX CONCURRENTLY %s %s",
            name, table, rows, name, definition,
        )
        return
    with op.get_context().autocommit_block():
        conn.execute(text("SET statement_timeout = 0"))
        conn.execute(text("SET lock_timeout = 0"))
        if state == "invalid":
            conn.execute(text(f"DROP INDEX CONCURRENTLY IF EXISTS {name}"))
        conn.execute(text(f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {name} {definition}"))
        conn.execute(text("RESET statement_timeout"))
        conn.execute(text("RESET lock_timeout"))


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '10s'")
    op.execute("SET LOCAL statement_timeout = '120s'")
    for stmt in _DDL:
        op.execute(stmt)

    # GRANT footer (migrations/_TEMPLATE.sql): alembic runs as the owner role;
    # the `grid` application role reads these tables (API) and writes them
    # (the job inserts a version and deletes superseded ones).
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grid') THEN
                EXECUTE 'GRANT ALL ON actor_graph_runs TO grid';
                EXECUTE 'GRANT ALL ON actor_graph_metrics TO grid';
                EXECUTE 'GRANT ALL ON actor_graph_communities TO grid';
                EXECUTE 'GRANT ALL ON actor_graph_edges TO grid';
            END IF;
        END
        $$
        """
    )

    _ensure_index(
        "idx_wealth_flows_to_entity", "wealth_flows",
        "ON wealth_flows (to_entity)", allow_concurrent=True,
    )
    conn = op.get_bind()
    has_trgm = conn.execute(text("SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm'")).fetchone()
    if has_trgm:
        _ensure_index(
            "idx_actors_name_trgm", "actors",
            "ON actors USING gin (name gin_trgm_ops)", allow_concurrent=False,
        )
    else:
        _log.warning("pg_trgm is not installed: actor search falls back to unindexed ILIKE")


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '10s'")
    for table in reversed(NEW_TABLES):
        op.execute(f"DROP TABLE IF EXISTS {table}")
    op.execute("DROP INDEX IF EXISTS idx_wealth_flows_to_entity")
