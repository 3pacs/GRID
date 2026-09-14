"""Promote analytical_snapshots' actor to a real column.

Revision ID: snapshot_actor_col_20260914
Revises: raw_sql_port_20260914
Create Date: 2026-09-14 18:00:00.000000

Four attempts, now, to make ``entity_resolver.py``'s search of
``analytical_snapshots`` fast:

* **Attempt 1** (deploy run 637) ran ``CREATE INDEX CONCURRENTLY`` on
  ``lower(COALESCE(payload ->> 'actor', payload ->> 'senator'))`` under
  ``db.py``'s default 120 s ``statement_timeout`` and was killed at two
  minutes.
* **Attempt 2** lifted ``statement_timeout`` to 30 min but set
  ``lock_timeout = 60s``. ``CONCURRENTLY``'s ``WaitForOlderSnapshots`` phase
  waits on a ``VIRTUALXACTID`` held by *every* backend with an older
  snapshot, database-wide, not only backends touching this table -- so
  Hermes' unrelated long-running scans counted against that 60 s and this
  would have failed too, for a different reason than attempt 1.
* **Attempt 3** (ops-exec run 288, 2026-09-13) ran the build out-of-band with
  ``statement_timeout`` at 1400 s and ``lock_timeout`` disabled -- every
  obstacle removed -- and the trigram build **still timed out** after the
  full 1400 s. ``migrations/versions/snapshot_actor_index_20260912.py`` is
  what that attempt produced: it gates on table size and, on griddb (1155 MB,
  over its 128 MB ceiling), runs no DDL at all and logs the two-statement
  remedy for a maintenance window instead. Confirmed still true moments ago
  (2026-09-14): ``idx_analytical_snapshots_payload_actor`` (176 kB) and
  ``idx_analytical_snapshots_payload_actor_trgm`` (480 kB) both exist with
  ``indisready=true, indisvalid=false`` -- Postgres pays to maintain them on
  every write, and the planner has never once used either.

Attempt 3's own conclusion is what this revision builds: *the cost was never
the row count, it was evaluating* ``payload ->> 'actor'`` *on every one of
558k+ rows to build a partial **expression** index* -- 245 MB of jsonb TOAST,
detoasted twice because ``CONCURRENTLY`` builds in two passes. A plain column
needs no jsonb extraction at index-build time, so that cost disappears
regardless of table size, and the byte-size deferral gate this revision's
predecessor needed no longer has anything to guard.

Confirmed read-only against griddb (2026-09-14, every statement below run
inside a transaction and rolled back -- see PR description for the session
transcript):

    payload ? 'actor'      9,649 rows  (id 557790-567438 -- only the tail,
                                          written since #477's fix)
    payload ? 'senator'    0 rows      -- fully retired; see below
    ALTER TABLE ... ADD COLUMN actor_name TEXT           0.0018 s
    UPDATE ... SET actor_name = payload ->> 'actor'
        WHERE payload ->> 'actor' IS NOT NULL             3.277 s, 9,649 rows
        (the entire table, id 1-568642, as ONE statement -- chunked below
        regardless, see "Chunking" below)
    CREATE INDEX ... ON analytical_snapshots (actor_name)
        WHERE actor_name IS NOT NULL AND actor_name <> '' 0.873 s
    CREATE INDEX ... USING gin (lower(actor_name) gin_trgm_ops)
        WHERE actor_name IS NOT NULL AND actor_name <> '' 0.643 s

Under five seconds, total, for the operation that previously could not finish
in 1400. That is the entire case for this design: remove the thing that was
actually expensive rather than work around it.

Why the predicate is ``payload ->> 'actor' IS NOT NULL``, not ``payload ?
'actor'``
---------------------------------------------------------------------------
jsonb key-existence looked like it should be cheaper than extract-then-check
-- no value materialization, just a key scan. Measured on the same 555000-
568642 id range (13,061 rows, 9,649 matches, warm cache, five-run average):
``payload ? 'actor'`` took *longer* (~165 ms) than ``payload ->> 'actor' IS
NOT NULL`` (~127 ms). Both still have to detoast the same jsonb value --
there is no partial-detoast for a compressed (``EXTENDED`` storage) jsonb
column -- so the extraction path is not paying a separate detoast cost the
existence check avoids, and ``->>`` benefits from being the exact expression
`SNAPSHOT_SEARCH_SQL` / `SNAPSHOT_NAME_SCAN_SQL` used to filter on before
this revision, and the exact one the backfill needs to compute the value
regardless. Reasoned about, then measured, per the task: guessing would have
picked the slower one.

Why ``senator`` is not read
----------------------------
``entity_resolver.py`` used to read
``COALESCE(payload ->> 'actor', payload ->> 'senator')`` -- ``senator`` being
the pre-#477 spelling of the same field, carried by the raw Senate EFD
records. Confirmed on griddb moments ago: 0 rows have that key. It is fully
retired, not merely rare, so the backfill and the write path (see
``scripts/parse_datasets.py::_snapshot_row``) both read only ``actor``.

Why this is one ordinary transaction, not ``CONCURRENTLY``
------------------------------------------------------------
Two independent reasons, either one sufficient on its own:

1. **Structural.** ``migrations/env.py`` opens exactly one
   ``context.begin_transaction()`` around the whole run, and no revision in
   this tree uses Alembic's ``autocommit_block()`` escape hatch.
   ``CREATE INDEX CONCURRENTLY`` cannot run inside a transaction block at
   all -- it would fail immediately with a driver error, before evaluating
   a single row, on *any* size table.
2. **Now unnecessary, but not lock-free.** A plain, non-``CONCURRENTLY``
   ``CREATE INDEX`` takes only a ``SHARE`` lock on its own -- it would block
   writers but not readers for the 0.87 s / 0.64 s measured above. But this
   revision runs as ONE transaction (see "Structural" above), and
   ``ADD COLUMN`` -- the first statement -- already takes ACCESS EXCLUSIVE
   and holds it, uninterrupted, until COMMIT: Postgres does not downgrade a
   lock mid-transaction because a later statement would have asked for less.
   So for this transaction's *entire* duration -- backfill, both index
   builds, both drops -- ``analytical_snapshots`` is inaccessible to every
   other reader and writer, not only to writers during the index builds. A
   prior draft of this paragraph described only the index builds' own lock
   and understated that.

   What bounds the exposure is duration, not lock strength: every statement
   in the transaction is either a catalog-only change or scans/writes a
   table with no jsonb extraction (see the top of this docstring for why
   that extraction was the actual cost). Measured end to end in production
   (deploy run 34872136777, 2026-09-14): ``ALTER TABLE`` to the transaction
   reaching ``head`` was 5.97 s, backfilling the same 9,649 rows measured in
   the dry run above -- consistent with the "under five seconds" dry-run
   estimate. That is an observed bound from one production run at that
   table's current size, not a guarantee at 10-100x it; ``BUILD_STATEMENT_
   TIMEOUT`` / ``BUILD_LOCK_TIMEOUT`` below exist precisely so a future
   regression fails loudly within 120s/30s instead of holding this lock
   indefinitely.

So there is no size gate here, unlike this revision's predecessor: every
step is cheap regardless of table size once the column is plain text, so
there is nothing left to defer -- "cheap" bounds the transaction's
duration, not its lock scope, which is ACCESS EXCLUSIVE throughout per
above.

Chunking
--------
The backfill is still batched by id range rather than run as the single
statement measured above, per the task's own instruction and as a hedge
against the table someday being larger than it is today: each chunk gets its
own bounded ``statement_timeout`` and touches a bounded number of rows, so a
future regression fails one chunk loudly with an id range to re-run rather
than hanging the whole migration. Under this transaction structure, chunking
does not give independent commit points (a mid-run failure still rolls back
everything Alembic applied in this invocation) -- see "Why this is one
ordinary transaction" above -- but it still bounds per-statement cost and
gives incremental progress in the log.

The two leftover indexes
--------------------------
``idx_analytical_snapshots_payload_actor`` and ``..._trgm`` are dropped
unconditionally by name, not only when ``indisvalid`` is false. On griddb
today both are INVALID leftovers from the failed attempts above. But on a
*fresh* database, the predecessor revision's small-table branch would have
built them successfully (they would be perfectly valid) moments before this
revision runs in the same ``alembic upgrade head`` invocation -- and they
would still be fully superseded by the columns this revision adds, since
``entity_resolver.py`` no longer references the jsonb expression they were
built on. Dropping by name regardless of validity is correct in both cases.
"""

from __future__ import annotations

import logging
from typing import Iterator, Sequence, Union

from alembic import op
from sqlalchemy import text


# revision identifiers, used by Alembic.
revision: str = "snapshot_actor_col_20260914"
down_revision: Union[str, Sequence[str], None] = "raw_sql_port_20260914"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

log = logging.getLogger("alembic.runtime.migration")

NEW_BTREE_INDEX = "idx_analytical_snapshots_actor_name"
NEW_TRGM_INDEX = "idx_analytical_snapshots_actor_name_trgm"

# The two INVALID leftovers from the CONCURRENTLY attempts this revision's
# predecessor documents. Dropped unconditionally -- see "The two leftover
# indexes" above.
LEGACY_BTREE_INDEX = "idx_analytical_snapshots_payload_actor"
LEGACY_TRGM_INDEX = "idx_analytical_snapshots_payload_actor_trgm"

# Must stay in step with SNAPSHOT_SEARCH_SQL / SNAPSHOT_NAME_SCAN_SQL in
# intelligence/entity_resolver.py: both filter on exactly this. A plain-
# column partial index is usable only when its predicate implies the
# query's, same rule as this revision's predecessor.
PREDICATE = "actor_name IS NOT NULL AND actor_name <> ''"

_CREATE_BTREE = f"""
    CREATE INDEX IF NOT EXISTS {NEW_BTREE_INDEX}
        ON analytical_snapshots (actor_name)
     WHERE {PREDICATE}
"""

_CREATE_TRGM = f"""
    CREATE INDEX IF NOT EXISTS {NEW_TRGM_INDEX}
        ON analytical_snapshots
     USING gin (lower(actor_name) gin_trgm_ops)
     WHERE {PREDICATE}
"""

# id-range width per backfill UPDATE. Measured at 3.277 s for the *entire*
# table (568k-wide id range) as one statement; this only exists to bound any
# single chunk's cost against future growth, not because today's table needs
# it. See "Chunking" above.
BACKFILL_CHUNK_SIZE = 50_000

# Generous relative to the ~5 s the whole operation measured at, so a future
# regression (table 10-100x its current size) fails loudly well before it
# could hang a deploy, rather than needing to be tuned tightly today.
BUILD_STATEMENT_TIMEOUT = "120s"
BUILD_LOCK_TIMEOUT = "30s"


def _table_exists(conn) -> bool:
    """True if ``analytical_snapshots`` exists.

    No migration creates this table -- ``store/snapshots.py``'s
    ``ANALYTICAL_SNAPSHOTS_DDL`` does, at runtime -- so a genuinely fresh
    database can reach this revision before it is there. Same fresh-database
    guard as this revision's predecessor.
    """
    return bool(
        conn.execute(
            text("SELECT to_regclass('analytical_snapshots') IS NOT NULL")
        ).scalar()
    )


def _id_bounds(conn) -> tuple[int, int] | None:
    """(min id, max id) of analytical_snapshots, or None if it has no rows."""
    row = conn.execute(text("SELECT min(id), max(id) FROM analytical_snapshots")).fetchone()
    if row is None or row[0] is None:
        return None
    return int(row[0]), int(row[1])


def _chunk_ranges(lo: int, hi: int, size: int) -> Iterator[tuple[int, int]]:
    """Yield contiguous, non-overlapping (start, end) id windows covering [lo, hi]."""
    start = lo
    while start <= hi:
        end = min(start + size - 1, hi)
        yield start, end
        start = end + 1


def _pg_trgm_ready(conn) -> bool:
    """Ensure pg_trgm is installed, returning whether the trigram index can build.

    Already installed on griddb (confirmed 2026-09-14). Same fallback this
    revision's predecessor uses, and ``intelligence/actor_discovery.py``'s own
    trigram index on ``actors(name)``: report the shortfall rather than fail
    the whole upgrade, because the trigram index is a performance index, not
    a correctness one.

    The ``CREATE EXTENSION`` attempt runs inside a SAVEPOINT
    (``conn.begin_nested()``). This whole revision runs as one transaction
    (see "Why this is one ordinary transaction" above), and Postgres aborts
    that transaction on a failed statement regardless of whether the driver
    exception is caught in Python -- every later statement in ``upgrade()``,
    including the ``RESET statement_timeout`` / ``RESET lock_timeout`` calls
    in its own ``finally`` block, would then fail with "current transaction
    is aborted, commands ignored until end of transaction block". The
    SAVEPOINT confines a failed ``CREATE EXTENSION`` to this one attempt.
    """
    installed = conn.execute(
        text("SELECT count(*) FROM pg_extension WHERE extname = 'pg_trgm'")
    ).scalar()
    if installed:
        return True

    try:
        with conn.begin_nested():
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
        return True
    except Exception as exc:
        log.warning(
            "pg_trgm unavailable (%s) - skipping %s. The equality arm of "
            "entity_resolver's snapshot search is still indexed, but its "
            "LIKE arm will scan analytical_snapshots sequentially. Install "
            "pg_trgm as superuser and re-run this revision to fix.",
            exc, NEW_TRGM_INDEX,
        )
        return False


def upgrade() -> None:
    """Add actor_name, backfill it, index it, retire the jsonb-expression indexes.

    In order: (a) add the column, (b) backfill from the jsonb payload in
    bounded id-range chunks, (c) build the two plain-column indexes, (d) drop
    the two indexes they supersede. See the module docstring for why none of
    this needs a size gate the way this revision's predecessor did.
    """
    conn = op.get_bind()

    if not _table_exists(conn):
        log.warning(
            "analytical_snapshots does not exist yet, so actor_name was not "
            "added and %s / %s were not created. No migration creates that "
            "table - store/snapshots.py does, on first use. Re-run this "
            "revision once the store has created it, or let the next deploy "
            "do so.",
            NEW_BTREE_INDEX, NEW_TRGM_INDEX,
        )
        return

    op.execute(f"SET statement_timeout = '{BUILD_STATEMENT_TIMEOUT}'")
    op.execute(f"SET lock_timeout = '{BUILD_LOCK_TIMEOUT}'")
    try:
        # (a) Metadata-only on PG11+ for a nullable column with no volatile
        # default - measured 0.0018 s against griddb. See module docstring.
        op.execute("ALTER TABLE analytical_snapshots ADD COLUMN IF NOT EXISTS actor_name TEXT")

        # (b) Chunked backfill. Idempotent: re-running assigns the same
        # values to already-backfilled rows.
        bounds = _id_bounds(conn)
        if bounds is None:
            log.info("analytical_snapshots has no rows yet; nothing to backfill")
        else:
            lo_id, hi_id = bounds
            total_updated = 0
            for chunk_lo, chunk_hi in _chunk_ranges(lo_id, hi_id, BACKFILL_CHUNK_SIZE):
                result = conn.execute(
                    text(
                        "UPDATE analytical_snapshots "
                        "   SET actor_name = payload ->> 'actor' "
                        " WHERE id BETWEEN :lo AND :hi "
                        "   AND payload ->> 'actor' IS NOT NULL"
                    ),
                    {"lo": chunk_lo, "hi": chunk_hi},
                )
                total_updated += result.rowcount
                log.info(
                    "actor_name backfill: id %s-%s, %s row(s) this chunk",
                    chunk_lo, chunk_hi, result.rowcount,
                )
            log.info(
                "actor_name backfill complete: %s row(s) across id %s-%s",
                total_updated, lo_id, hi_id,
            )

        # (c) New plain-column indexes.
        op.execute(_CREATE_BTREE)
        if _pg_trgm_ready(conn):
            op.execute(_CREATE_TRGM)

        # (d) Retire the jsonb-expression indexes they supersede. See "The
        # two leftover indexes" in the module docstring for why this is
        # unconditional rather than indisvalid-gated.
        for name in (LEGACY_TRGM_INDEX, LEGACY_BTREE_INDEX):
            op.execute(f'DROP INDEX IF EXISTS "{name}"')
    finally:
        op.execute("RESET statement_timeout")
        op.execute("RESET lock_timeout")


def downgrade() -> None:
    """Drop what this revision added. Nothing depends on any of it.

    Does not resurrect the jsonb-expression indexes dropped in ``upgrade()``
    -- they were dead weight (INVALID on griddb; see module docstring), not
    something a downgrade should restore.
    """
    op.execute(f"SET lock_timeout = '{BUILD_LOCK_TIMEOUT}'")
    try:
        op.execute(f"DROP INDEX IF EXISTS {NEW_TRGM_INDEX}")
        op.execute(f"DROP INDEX IF EXISTS {NEW_BTREE_INDEX}")
        op.execute("ALTER TABLE analytical_snapshots DROP COLUMN IF EXISTS actor_name")
    finally:
        op.execute("RESET lock_timeout")
