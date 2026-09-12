"""Index analytical_snapshots on the payload actor.

Revision ID: snapshot_payload_actor_index_20260912
Revises: restore_news_search_arm_20260912
Create Date: 2026-09-12 01:10:00.000000

``intelligence/entity_resolver.py`` resolves an entity by searching every
source table for its name. Its ``analytical_snapshots`` arm used to select an
``actor`` column, which the table has never had (#477), so it raised
``UndefinedColumn`` on every call and contributed nothing. Now that it reads
the real location — the jsonb ``payload`` — the query runs, and on griddb it
plans as a sequential scan of all 556,061 rows:

    Limit  (cost=0.00..11417.45 rows=200 width=66)
      ->  Seq Scan on analytical_snapshots  (cost=0.00..128160.85 rows=2245)
            Filter: (((payload ->> 'actor') IS NOT NULL) AND
                     ((lower(payload ->> 'actor') = 'david perdue') OR
                      (lower(payload ->> 'actor') ~~ '%perdue%')))

measured 2026-09-12 (the plan is the same shape for the COALESCE below). That
is once per entity resolved, and ``build_resolution_index`` resolves thousands
in a run.

The index is **partial**, matching the two queries' own predicate. Actors are
a thin slice of this table: the bulk is ``crypto_label`` (169,667),
``llm_task_*`` and ``sector_flows``, none of which carry an actor. Indexing
only the rows that do keeps it small and makes the name scan in
``build_resolution_index`` — ``SELECT DISTINCT ... WHERE ... IS NOT NULL AND
<> ''`` — read an index instead of the whole table.

The key is ``lower(COALESCE(payload ->> 'actor', payload ->> 'senator'))``
because that is the expression both queries compare against: ``actor`` is the
canonical key ``parse_datasets`` writes since #477 (0 rows so far — every one
of those writes used to fail), and ``senator`` is the same field under its
pre-#477 spelling, carried by the 5,000 live ``congressional_trade`` rows. A
partial index is only usable when its predicate implies the query's, so the
two must stay in step; ``SNAPSHOT_SEARCH_SQL`` in
``intelligence/entity_resolver.py`` is the other half.

The equality arm gets an index condition; the ``LIKE '%token%'`` arm cannot
use a btree at all (leading wildcard), but it rides the same partial index, so
it filters a few thousand entries rather than 556k rows.

``CONCURRENTLY`` is a deliberate departure from the plain ``CREATE INDEX`` the
other migrations in this tree use. Those build indexes on tables they have
just created; this one is a live table that Hermes writes to on every cycle,
and a plain build would hold ACCESS EXCLUSIVE for a full heap scan.
"""

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "snapshot_payload_actor_index_20260912"
down_revision: Union[str, Sequence[str], None] = "restore_news_search_arm_20260912"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

INDEX_NAME = "idx_analytical_snapshots_payload_actor"

# Must stay in step with SNAPSHOT_SEARCH_SQL / SNAPSHOT_NAME_SCAN_SQL in
# intelligence/entity_resolver.py — a partial index is only usable when its
# predicate implies the query's.
ACTOR_EXPR = "COALESCE(payload ->> 'actor', payload ->> 'senator')"

_CREATE_INDEX = f"""
    CREATE INDEX CONCURRENTLY IF NOT EXISTS {INDEX_NAME}
        ON analytical_snapshots (lower({ACTOR_EXPR}))
     WHERE {ACTOR_EXPR} IS NOT NULL
       AND {ACTOR_EXPR} <> ''
"""

# A CONCURRENTLY build that fails partway leaves the index behind marked
# invalid. IF NOT EXISTS would then skip it forever and the planner would
# never use it, so clear an invalid leftover before rebuilding.
_DROP_IF_INVALID = f"""
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM pg_class c
          JOIN pg_index i ON i.indexrelid = c.oid
         WHERE c.relname = '{INDEX_NAME}'
           AND NOT i.indisvalid
    ) THEN
        EXECUTE 'DROP INDEX {INDEX_NAME}';
        RAISE NOTICE 'dropped invalid {INDEX_NAME} left by an earlier build';
    END IF;
END
$$;
"""


def upgrade() -> None:
    """Create the partial expression index, without locking out writers."""
    # CREATE INDEX CONCURRENTLY cannot run inside a transaction block.
    with op.get_context().autocommit_block():
        op.execute(_DROP_IF_INVALID)
        op.execute(_CREATE_INDEX)


def downgrade() -> None:
    """Drop the index. Nothing depends on it; the queries just get slower."""
    with op.get_context().autocommit_block():
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {INDEX_NAME}")
