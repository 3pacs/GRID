"""Index analytical_snapshots on the payload actor.

Revision ID: snapshot_payload_actor_index_20260912
Revises: restore_news_search_arm_20260912
Create Date: 2026-09-12 01:10:00.000000

``intelligence/entity_resolver.py`` resolves an entity by searching every
source table for its name. Its ``analytical_snapshots`` arm used to select an
``actor`` column, which the table has never had (#477), so it raised
``UndefinedColumn`` on every call and contributed nothing. Now that it reads
the real location — the jsonb ``payload`` — the query runs, once per entity
resolved, and ``build_resolution_index`` resolves thousands per run.

Two indexes, because the query's WHERE clause is an OR and each arm needs a
different access method:

    WHERE (lower(<actor>) = lower(:name)     <- btree
           OR lower(<actor>) LIKE :pattern)  <- leading wildcard: trigram only

Neither index is worth anything on its own. Measured on griddb 2026-09-12,
all plans against the live 556,061-row table, probe indexes rolled back:

    no index        Limit  (cost=1000.00..9414)
                      Parallel Seq Scan  (cost=0.00..121197.43)

    btree only      Limit  (cost=1034.56..9364)
                      Parallel Bitmap Heap Scan  (cost=34.56..121189.52)
                        Bitmap Index Scan on <btree>  (rows=548368)
                    -- the index supplies only the partial predicate, the OR
                    -- is still a filter over the whole table. No gain.

    btree + trigram Limit  (cost=36.81..262.72)
                      Bitmap Heap Scan  (cost=36.81..3344.07)
                        BitmapOr
                          Bitmap Index Scan on <btree>
                            Index Cond: lower(<actor>) = 'david a perdue , jr'
                          Bitmap Index Scan on <trigram>
                            Index Cond: lower(<actor>) ~~ '%perdue%'

121,197 -> 3,344 on the scan, 9,364 -> 263 on the Limit: a BitmapOr that
actually indexes both arms. Sizes on griddb today: btree 56 kB, trigram
232 kB.

Both are **partial**, matching the queries' own predicate — a partial index is
only usable when its predicate implies the query's, so
``SNAPSHOT_SEARCH_SQL`` in ``intelligence/entity_resolver.py`` is the other
half of this and the two must stay in step. Actors are a thin slice of this
table: the bulk is ``crypto_label`` (169,667), ``llm_task_*`` and
``sector_flows``, none of which carry one.

The key is ``lower(COALESCE(payload ->> 'actor', payload ->> 'senator'))``:
``actor`` is the canonical key ``parse_datasets`` writes since #477 (0 rows so
far — every one of those writes used to fail), ``senator`` the same field
under its pre-#477 spelling, carried by the 5,000 live ``congressional_trade``
rows.

Not indexed: the ``SELECT DISTINCT`` name scan in ``build_resolution_index``.
It stays a sequential scan under every variant above, including with
``enable_seqscan=off`` — it reads the raw value for every qualifying row, and
PostgreSQL cannot estimate the selectivity of a COALESCE over jsonb without
extended statistics, so it plans for ~548k rows rather than the true ~5k.
That scan is a rare batch operation and one sequential pass is the right cost
for it.

``CONCURRENTLY`` is a deliberate departure from the plain ``CREATE INDEX`` the
other migrations in this tree use. Those build indexes on tables they have
just created; this one is live and Hermes writes to it every cycle, so a
plain build would hold ACCESS EXCLUSIVE for a full heap scan.
"""

import logging
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "snapshot_payload_actor_index_20260912"
down_revision: Union[str, Sequence[str], None] = "restore_news_search_arm_20260912"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

log = logging.getLogger("alembic.runtime.migration")

BTREE_INDEX = "idx_analytical_snapshots_payload_actor"
TRGM_INDEX = "idx_analytical_snapshots_payload_actor_trgm"

# Must stay in step with SNAPSHOT_SEARCH_SQL in
# intelligence/entity_resolver.py.
ACTOR_EXPR = "COALESCE(payload ->> 'actor', payload ->> 'senator')"
PREDICATE = f"{ACTOR_EXPR} IS NOT NULL AND {ACTOR_EXPR} <> ''"

_CREATE_BTREE = f"""
    CREATE INDEX CONCURRENTLY IF NOT EXISTS {BTREE_INDEX}
        ON analytical_snapshots (lower({ACTOR_EXPR}))
     WHERE {PREDICATE}
"""

_CREATE_TRGM = f"""
    CREATE INDEX CONCURRENTLY IF NOT EXISTS {TRGM_INDEX}
        ON analytical_snapshots
     USING gin (lower({ACTOR_EXPR}) gin_trgm_ops)
     WHERE {PREDICATE}
"""


def _drop_if_invalid(index_name: str) -> str:
    """SQL that clears an index left INVALID by a failed CONCURRENTLY build.

    ``IF NOT EXISTS`` would otherwise skip such a leftover forever and the
    planner would never use it.
    """
    return f"""
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM pg_class c
          JOIN pg_index i ON i.indexrelid = c.oid
         WHERE c.relname = '{index_name}'
           AND NOT i.indisvalid
    ) THEN
        EXECUTE 'DROP INDEX {index_name}';
        RAISE NOTICE 'dropped invalid {index_name} left by an earlier build';
    END IF;
END
$$;
"""


def _pg_trgm_ready(conn) -> bool:
    """Ensure pg_trgm is installed, returning whether the trigram index can build.

    Already installed on griddb. On a database where it is missing, creating
    it needs superuser (pg_trgm is not a trusted extension), so this reports
    the shortfall instead of failing the whole upgrade — the trigram index is
    a performance index, not a correctness one. The same fallback
    ``intelligence/actor_discovery.py`` already uses for its own trigram index
    on ``actors(name)``.
    """
    from sqlalchemy import text

    installed = conn.execute(
        text("SELECT count(*) FROM pg_extension WHERE extname = 'pg_trgm'")
    ).scalar()
    if installed:
        return True

    try:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
        return True
    except Exception as exc:
        log.warning(
            "pg_trgm unavailable (%s) — skipping %s. The equality arm of "
            "entity_resolver's snapshot search is still indexed, but its "
            "LIKE arm will scan analytical_snapshots sequentially. Install "
            "pg_trgm as superuser and re-run this revision to fix.",
            exc, TRGM_INDEX,
        )
        return False


def upgrade() -> None:
    """Create both partial expression indexes, without locking out writers."""
    conn = op.get_bind()
    # CREATE INDEX CONCURRENTLY cannot run inside a transaction block.
    with op.get_context().autocommit_block():
        op.execute(_drop_if_invalid(BTREE_INDEX))
        op.execute(_CREATE_BTREE)

        if _pg_trgm_ready(conn):
            op.execute(_drop_if_invalid(TRGM_INDEX))
            op.execute(_CREATE_TRGM)


def downgrade() -> None:
    """Drop both indexes. Nothing depends on them; the search just gets slower."""
    with op.get_context().autocommit_block():
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {TRGM_INDEX}")
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {BTREE_INDEX}")
