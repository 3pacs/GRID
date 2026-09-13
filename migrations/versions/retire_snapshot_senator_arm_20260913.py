"""Retire the payload senator fallback from the analytical_snapshots actor index.

Revision ID: retire_snapshot_senator_arm_20260913
Revises: snapshot_payload_actor_index_20260912
Create Date: 2026-09-13 06:50:00.000000

Handoff GRID-HANDOFF-snapshot-actor-reload-2026-09-13 deleted the 5,000
`payload ->> 'senator'`-keyed `congressional_trade` rows on griddb and
reloaded them under the canonical `payload ->> 'actor'` key (parse_datasets
has written `actor` since #477). Measured immediately after:
`payload ->> 'senator'` is 0 rows, `payload ->> 'actor'` is 9,649. No writer
in the tree has used `senator` since #477, so the COALESCE fallback in both
the index and ``intelligence/entity_resolver.py``'s
``SNAPSHOT_SEARCH_SQL`` / ``SNAPSHOT_NAME_SCAN_SQL`` is dead weight.

This does **not** edit ``snapshot_payload_actor_index_20260912`` in place.
Alembic tracks applied state by revision id, not by diffing file content —
editing an already-applied migration's source has no effect on a database
that already ran it (as this one already had, ahead of ``alembic_version``:
the btree index existed on griddb before this repo's migration history
recorded it applied). The only way to actually change the live index is a
new revision.

Same two-index, `CONCURRENTLY` shape as the migration this supersedes, and
the same reasoning applies: the btree and trigram indexes must key on
*exactly* the expression the query filters on, or PostgreSQL's partial-index
usability check silently fails and the planner falls back to the
121,197-cost sequential scan the prior migration measured — no error, just a
resolver that got slower. ``tests/test_entity_resolver_snapshots.py::
test_partial_index_predicate_matches_the_queries`` now pins this file's
``ACTOR_EXPR`` against ``SNAPSHOT_SEARCH_SQL`` / ``SNAPSHOT_NAME_SCAN_SQL``
instead of the superseded migration's.

The old index names are kept (`idx_analytical_snapshots_payload_actor` /
`_trgm`) rather than issuing new ones — there is exactly one actor index per
access method and no reason to carry two generations of it.
"""

import logging
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "retire_snapshot_senator_arm_20260913"
down_revision: Union[str, Sequence[str], None] = "snapshot_payload_actor_index_20260912"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

log = logging.getLogger("alembic.runtime.migration")

BTREE_INDEX = "idx_analytical_snapshots_payload_actor"
TRGM_INDEX = "idx_analytical_snapshots_payload_actor_trgm"

# Must stay in step with SNAPSHOT_SEARCH_SQL / SNAPSHOT_NAME_SCAN_SQL in
# intelligence/entity_resolver.py.
ACTOR_EXPR = "payload ->> 'actor'"
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
    """SQL that clears an index left INVALID by a failed CONCURRENTLY build."""
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
    """Ensure pg_trgm is installed, returning whether the trigram index can build."""
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
            "LIKE arm will scan analytical_snapshots sequentially.",
            exc, TRGM_INDEX,
        )
        return False


def upgrade() -> None:
    """Drop the senator-COALESCE indexes and rebuild on the bare actor key.

    `DROP INDEX CONCURRENTLY` cannot run inside the same transaction as the
    matching `CREATE`, so each pair gets its own autocommit statement — same
    constraint that makes this whole migration run outside a transaction
    block.
    """
    conn = op.get_bind()
    with op.get_context().autocommit_block():
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {BTREE_INDEX}")
        op.execute(_drop_if_invalid(BTREE_INDEX))
        op.execute(_CREATE_BTREE)

        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {TRGM_INDEX}")
        if _pg_trgm_ready(conn):
            op.execute(_drop_if_invalid(TRGM_INDEX))
            op.execute(_CREATE_TRGM)


def downgrade() -> None:
    """Revert to the COALESCE(actor, senator) expression.

    Correct only because the reload that motivated this migration is a data
    fact, not a schema fact: `senator` rows stay retired regardless of which
    index expression is live, so this just restores the wider, now-equivalent
    predicate rather than resurrecting any data.
    """
    from migrations.versions.snapshot_payload_actor_index_20260912 import (  # noqa: E501
        ACTOR_EXPR as OLD_ACTOR_EXPR,
    )

    old_predicate = f"{OLD_ACTOR_EXPR} IS NOT NULL AND {OLD_ACTOR_EXPR} <> ''"
    conn = op.get_bind()
    with op.get_context().autocommit_block():
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {BTREE_INDEX}")
        op.execute(
            f"""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS {BTREE_INDEX}
                ON analytical_snapshots (lower({OLD_ACTOR_EXPR}))
             WHERE {old_predicate}
            """
        )
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {TRGM_INDEX}")
        if _pg_trgm_ready(conn):
            op.execute(
                f"""
                CREATE INDEX CONCURRENTLY IF NOT EXISTS {TRGM_INDEX}
                    ON analytical_snapshots
                 USING gin (lower({OLD_ACTOR_EXPR}) gin_trgm_ops)
                 WHERE {old_predicate}
                """
            )
