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
                    -- the index supplies only the partial predicate, the OR
                    -- is still a filter over the whole table. No gain.

    btree + trigram Limit  (cost=36.81..262.72)
                      Bitmap Heap Scan  (cost=36.81..3344.07)
                        BitmapOr
                          Bitmap Index Scan on <btree>
                          Bitmap Index Scan on <trigram>

Both are **partial**, matching the queries' own predicate — a partial index is
only usable when its predicate implies the query's, so
``SNAPSHOT_SEARCH_SQL`` in ``intelligence/entity_resolver.py`` is the other
half of this and the two must stay in step.

The key is ``lower(COALESCE(payload ->> 'actor', payload ->> 'senator'))``:
``actor`` is the canonical key ``parse_datasets`` writes since #477,
``senator`` the same field under its pre-#477 spelling, carried by the live
``congressional_trade`` rows.

Why this revision does not build them on a large table
------------------------------------------------------

Three attempts on griddb, and the third settled it.

**Attempt 1** (deploy run 637) used ``CREATE INDEX CONCURRENTLY`` under
``db.py``'s default 120 s ``statement_timeout``, and was killed at two
minutes.

**Attempt 2** lifted ``statement_timeout`` to 30 min but set
``lock_timeout = 60s``. That would have failed too, for a different reason:
``CONCURRENTLY``'s waits for other transactions (``WaitForLockers`` after
each build phase, ``WaitForOlderSnapshots`` before the index is marked valid)
are lock acquisitions on ``VIRTUALXACTID`` tags, so ``lock_timeout`` governs
them — and ``WaitForOlderSnapshots`` waits on *every* backend in the database
holding an older snapshot, not only those touching this table, so Hermes'
``raw_series`` scan counts even though it never reads ``analytical_snapshots``.

**Attempt 3** (ops-exec run 288, 2026-09-13) ran the build out-of-band with
``statement_timeout`` at 1400 s and ``lock_timeout`` disabled. The trigram
index **still timed out**, after the full 1400 s:

    canceling statement due to statement timeout

That is the measurement this revision is built on. The reason is the
predicate, not the index. ``analytical_snapshots`` is 1155 MB (910 MB heap +
245 MB TOAST) over 558,235 rows, and a *partial* index must evaluate
``payload ->> 'actor'`` on every one of them, which detoasts every payload.
``CONCURRENTLY`` does that twice. Only 9,649 rows reach the index; the entire
cost is in getting to them.

So ``CONCURRENTLY`` is not viable on this table, and the alternative is worse
in an automated deploy: a plain ``CREATE INDEX`` holds ACCESS EXCLUSIVE for
the whole build. The same run showed a plain ``DROP INDEX`` could not even
acquire that lock within 5 s — Hermes writes this table continuously — so a
multi-minute ACCESS EXCLUSIVE hold would be a self-inflicted outage on every
deploy that ran it.

What it does instead
--------------------

Build inline where that is cheap, and refuse loudly where it is not:

* **Small table** (under ``INLINE_BUILD_MAX_BYTES``) — a fresh database, dev,
  CI, a restored slice — plain ``CREATE INDEX``. Fast, and the brief lock
  costs nothing when nothing else is connected. These environments get the
  indexes automatically, which a "leave it to the operator" rule would deny
  them.

* **Large table** — skip, and ``log.warning`` the exact SQL to run. The
  revision is still recorded as applied, because the *schema* is correct
  either way: these are performance indexes, and ``entity_resolver`` returns
  the same rows without them, just via a 121,197-cost sequential scan.

Skipping is deliberately **loud and tracked**, not silent. Silence is what
made the original bug (#477/#479) survive for months: a failure nobody could
see. A warning naming the consequence and the remedy is the opposite of that,
and the follow-up is tracked in the vault lane for 2026-09-13.

Size is a proxy for detoast volume, which is what actually costs. It does not
have to be exact — it only has to separate "a database small enough that a
brief exclusive lock is free" from "production".

The maintenance-window procedure for a large table is the warning's text:
run the two statements below during a window, with ``statement_timeout``
lifted, having first dropped any INVALID leftovers. Do it as
``CREATE INDEX`` (not ``CONCURRENTLY``) — one heap scan instead of two, and
no waiting on other transactions.
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

# Above this, an inline build is refused — see "What it does instead". griddb
# is 1155 MB; a dev or CI database is single-digit MB. Anything between is
# small enough that a brief ACCESS EXCLUSIVE hold is not an outage.
INLINE_BUILD_MAX_BYTES = 128 * 1024 * 1024

# Generous, because even a small table has to evaluate the predicate over
# every row. Still finite: a migration that never returns is worse than one
# that fails loudly.
BUILD_STATEMENT_TIMEOUT = "600s"

# Short, and correct precisely because these are plain (ACCESS EXCLUSIVE)
# statements: a waiting ACCESS EXCLUSIVE request queues every later query
# behind it, so failing fast is the safe outcome. This is the opposite of what
# CONCURRENTLY needs, which is why attempt 2 above got it backwards.
BUILD_LOCK_TIMEOUT = "10s"

_CREATE_BTREE = f"""
    CREATE INDEX IF NOT EXISTS {BTREE_INDEX}
        ON analytical_snapshots (lower({ACTOR_EXPR}))
     WHERE {PREDICATE}
"""

_CREATE_TRGM = f"""
    CREATE INDEX IF NOT EXISTS {TRGM_INDEX}
        ON analytical_snapshots
     USING gin (lower({ACTOR_EXPR}) gin_trgm_ops)
     WHERE {PREDICATE}
"""


def _table_bytes(conn) -> int:
    """Total size of analytical_snapshots, or 0 if it does not exist yet."""
    from sqlalchemy import text

    return conn.execute(text("""
        SELECT COALESCE(pg_total_relation_size(to_regclass('analytical_snapshots')), 0)
    """)).scalar() or 0


def _drop_if_invalid(conn, index_name: str) -> None:
    """Clear an index left INVALID by an earlier failed CONCURRENTLY build.

    ``IF NOT EXISTS`` matches on the *name*, so it skips such a leftover
    forever and the planner never gets a usable index — exactly what happened
    on griddb in ops-exec run 288, where the CREATE reported success having
    silently done nothing.

    ``DROP INDEX CONCURRENTLY`` rather than a plain DROP: it takes SHARE
    UPDATE EXCLUSIVE instead of ACCESS EXCLUSIVE, so it cannot pile queries up
    behind it on a busy table. The same run showed a plain DROP failing to get
    ACCESS EXCLUSIVE within 5 s here.
    """
    from sqlalchemy import text

    invalid = conn.execute(text("""
        SELECT 1 FROM pg_class c
          JOIN pg_index i ON i.indexrelid = c.oid
         WHERE c.relname = :name AND NOT i.indisvalid
    """), {"name": index_name}).scalar()
    if not invalid:
        return

    log.warning("%s is INVALID (a failed earlier build) - dropping it first",
                index_name)
    with op.get_context().autocommit_block():
        # No ACCESS EXCLUSIVE here, so lock_timeout must not cut short the
        # wait for older transactions, which is a VIRTUALXACTID lock wait.
        op.execute("SET lock_timeout = '0'")
        try:
            op.execute(f'DROP INDEX CONCURRENTLY IF EXISTS "{index_name}"')
        finally:
            op.execute("RESET lock_timeout")


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
            "pg_trgm unavailable (%s) - skipping %s. The equality arm of "
            "entity_resolver's snapshot search is still indexed, but its "
            "LIKE arm will scan analytical_snapshots sequentially. Install "
            "pg_trgm as superuser and re-run this revision to fix.",
            exc, TRGM_INDEX,
        )
        return False


def _warn_deferred(size_bytes: int, statements: Sequence[str]) -> None:
    """Say, in full, what was not built and exactly how to build it."""
    log.warning(
        "analytical_snapshots is %.0f MB, over the %.0f MB inline-build "
        "ceiling, so %s and %s were NOT created. entity_resolver's snapshot "
        "search still returns correct rows, via a sequential scan measured at "
        "cost 121,197 instead of 3,344. CREATE INDEX CONCURRENTLY has been "
        "measured as unable to finish on this table (ops-exec run 288: the "
        "trigram build timed out after 1400 s), so build these in a "
        "maintenance window, with statement_timeout lifted, as plain "
        "CREATE INDEX:\n%s",
        size_bytes / 1024 / 1024,
        INLINE_BUILD_MAX_BYTES / 1024 / 1024,
        BTREE_INDEX, TRGM_INDEX,
        "\n".join(" ".join(s.split()) + ";" for s in statements),
    )


def upgrade() -> None:
    """Create both partial expression indexes where that is cheap; else warn."""
    conn = op.get_bind()

    _drop_if_invalid(conn, BTREE_INDEX)
    _drop_if_invalid(conn, TRGM_INDEX)

    statements = [_CREATE_BTREE]
    if _pg_trgm_ready(conn):
        statements.append(_CREATE_TRGM)

    size_bytes = _table_bytes(conn)
    if size_bytes > INLINE_BUILD_MAX_BYTES:
        _warn_deferred(size_bytes, statements)
        return

    # Plain CREATE INDEX: one heap scan, no waiting on other transactions. The
    # ACCESS EXCLUSIVE hold is why this branch is gated on size.
    op.execute(f"SET statement_timeout = '{BUILD_STATEMENT_TIMEOUT}'")
    op.execute(f"SET lock_timeout = '{BUILD_LOCK_TIMEOUT}'")
    try:
        for statement in statements:
            op.execute(statement)
    finally:
        op.execute("RESET statement_timeout")
        op.execute("RESET lock_timeout")


def downgrade() -> None:
    """Drop both indexes. Nothing depends on them; the search just gets slower."""
    op.execute(f"SET lock_timeout = '{BUILD_LOCK_TIMEOUT}'")
    try:
        op.execute(f"DROP INDEX IF EXISTS {TRGM_INDEX}")
        op.execute(f"DROP INDEX IF EXISTS {BTREE_INDEX}")
    finally:
        op.execute("RESET lock_timeout")
