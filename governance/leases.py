"""
GRID cross-process research write leases (W4f, 2026-09-18).

Closes the gap documented in
``docs/handoffs/2026-09-18/fable-w4b-runstate.md`` ("What still lets a
worker write after loss of ownership"): the in-process
``scripts.hermes_operator._AutoresearchGenerationTracker`` fences a stale
worker THREAD within one Python process, but cannot fence a second
process, or a worker that survives a process restart. This module is the
DB-backed lease that closes that hole.

Design
------
``research_leases`` has exactly one row per lease *name* (e.g.
``"autoresearch"``). Whoever holds the row's current, unexpired
``generation`` is the owner; every real write the research loop makes is
wrapped in :func:`guarded_write` (or the psycopg2-flavored
:func:`guarded_write_dbapi`), which:

  1. Takes a row lock on the lease with ``SELECT ... FOR UPDATE`` --
     *first*, before anything else.
  2. Verifies the locked row's ``generation`` still equals the caller's
     ``generation`` and that ``expires_at`` is still in the future.
  3. Only if both hold does it invoke the caller's write function, in the
     SAME transaction as the lock.

Postgres row locks are held for the lifetime of the enclosing transaction,
not just the statement that took them -- so between step 1 and the
transaction's eventual COMMIT/ROLLBACK, no other transaction can modify
this row (a concurrent :func:`acquire`/:func:`heartbeat`/:func:`release`,
or another :func:`guarded_write` call, all take the same ``FOR UPDATE``
lock and block until this one ends). That is what makes "a write that
started before ownership was lost commits after the loss" impossible: the
generation bump that would represent "ownership lost" cannot itself commit
until this transaction is done, and by then this transaction has either
already committed its write (while it still legitimately held the lease)
or has been rolled back by :class:`OwnershipLost` before the write ever
ran. There is no interleaving in which the check passes, the row then
changes, and the write still lands -- the row cannot change while the
check-and-write transaction holds its lock.

Two call-site flavors are provided because this codebase's research loop
writes through two different DB layers:

  * SQLAlchemy (``validation/backtest.py``'s ``self.engine``): use
    :func:`guarded_write` / :func:`run_guarded`.
  * Raw psycopg2 (``scripts/autoresearch.py``'s ``pg``/``cur``): use
    :func:`guarded_write_dbapi` / :func:`run_guarded_dbapi`.

Both flavors lock and check the SAME ``research_leases`` row in the SAME
Postgres database, so the ordering guarantee above holds uniformly
regardless of which driver a given write happens to use.

Schema: see ``migrations/versions/research_leases_0918.py`` (revision
``research_leases_0918``, down_revision ``promotion_ledger_0918``).
``RESEARCH_LEASES_DDL`` here is the single source of truth for the table
shape, mirroring the pattern in ``governance/promotion_ledger.py``.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Callable, TypeVar

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

T = TypeVar("T")

TABLE_NAME = "research_leases"

# Single source of truth for the table shape -- the migration executes this
# verbatim (governance/promotion_ledger.py's pattern).
RESEARCH_LEASES_DDL: list[str] = [
    """
    CREATE TABLE IF NOT EXISTS research_leases (
        name TEXT PRIMARY KEY,
        owner_id TEXT NOT NULL,
        generation BIGINT NOT NULL,
        acquired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        expires_at TIMESTAMPTZ NOT NULL
    )
    """,
]

# Default time-to-live for a freshly acquired or heartbeated lease. Short
# relative to AUTORESEARCH_TIMEOUT_SECONDS (1800s default) on purpose: the
# operator is expected to heartbeat every HEARTBEAT_INTERVAL_SECONDS (see
# scripts/hermes_operator.py's _run_autoresearch_with_lease_heartbeat) while
# a run is legitimately still in progress, so a short TTL just means an
# abandoned lease goes stale quickly once heartbeats stop -- not that a
# healthy long run is at risk of expiring mid-write.
DEFAULT_LEASE_TTL_SECONDS = 45


class OwnershipLost(RuntimeError):
    """Raised by guarded_write()/guarded_write_dbapi() when the caller's
    generation is no longer the lease's current, unexpired generation.

    The transaction the check ran inside is rolled back by the caller's own
    transaction-management (an exception propagating out of
    ``engine.begin()`` rolls back automatically; ``run_guarded_dbapi``
    explicitly calls ``.rollback()`` in its except clause) -- so no
    statement `fn` would have executed ever gets a chance to commit.
    """

    def __init__(self, lease_name: str, generation: int, current_generation: int | None):
        self.lease_name = lease_name
        self.generation = generation
        self.current_generation = current_generation
        super().__init__(
            f"OwnershipLost: lease {lease_name!r} generation {generation} is "
            f"no longer current (current={current_generation})"
        )


class LeaseHeld(RuntimeError):
    """Raised by acquire() when the named lease is currently held by a live
    (unexpired) owner, so the ON CONFLICT ... WHERE expires_at < NOW()
    clause did not fire. This is a normal, expected outcome (e.g. two
    hermes_operator processes both reaching the cycle-6 gate), not a bug --
    the caller should skip this cycle's autoresearch invocation rather than
    force a takeover."""

    def __init__(self, lease_name: str, owner_id: str | None, generation: int | None, expires_at: Any):
        self.lease_name = lease_name
        self.owner_id = owner_id
        self.generation = generation
        self.expires_at = expires_at
        super().__init__(
            f"LeaseHeld: lease {lease_name!r} is held by owner_id={owner_id!r} "
            f"(generation={generation}, expires_at={expires_at}); not expired"
        )


def ensure_schema(engine: Engine) -> None:
    """Create research_leases if it doesn't exist (idempotent).

    Mirrors governance/promotion_ledger.py::ensure_schema -- used by tests
    against a real (disposable) Postgres so both that path and the Alembic
    revision run the exact same DDL. Not used against sqlite: the DDL uses
    Postgres-only types (TIMESTAMPTZ) and guarded_write's ``FOR UPDATE``
    requires real row locking semantics sqlite does not provide.
    """
    with engine.begin() as conn:
        for stmt in RESEARCH_LEASES_DDL:
            conn.execute(text(stmt))


# ── Acquire / heartbeat / release ───────────────────────────────────────

def acquire(
    engine: Engine,
    lease_name: str,
    owner_id: str,
    ttl_seconds: int = DEFAULT_LEASE_TTL_SECONDS,
) -> int:
    """Acquire (or take over) the named lease, minting a new generation.

    Implemented as a single ``INSERT ... ON CONFLICT (name) DO UPDATE ...
    WHERE research_leases.expires_at < NOW()`` -- the row is only taken
    over if it has actually expired (or does not exist yet, in which case
    the plain INSERT applies and there is no conflict at all). If the row
    exists and is NOT expired, the DO UPDATE's WHERE clause excludes it, so
    the statement performs no write and RETURNING yields no row for it --
    :func:`fetchone` returns ``None`` and this raises :class:`LeaseHeld`.

    Returns the new generation number on success.
    """
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(seconds=ttl_seconds)
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                INSERT INTO research_leases
                    (name, owner_id, generation, acquired_at, heartbeat_at, expires_at)
                VALUES
                    (:name, :owner, 1, :now, :now, :expires_at)
                ON CONFLICT (name) DO UPDATE SET
                    owner_id = EXCLUDED.owner_id,
                    generation = research_leases.generation + 1,
                    acquired_at = EXCLUDED.acquired_at,
                    heartbeat_at = EXCLUDED.heartbeat_at,
                    expires_at = EXCLUDED.expires_at
                WHERE research_leases.expires_at < :now
                RETURNING generation
                """
            ),
            {"name": lease_name, "owner": owner_id, "now": now, "expires_at": expires_at},
        ).fetchone()

        if row is not None:
            log.info(
                "Acquired research lease {n} for owner={o} (generation={g}, ttl={t}s)",
                n=lease_name, o=owner_id, g=row[0], t=ttl_seconds,
            )
            return row[0]

        # Someone else holds a live lease -- fetch it (read-only, no lock
        # needed: we already failed to take it, this is just for the error
        # message) to report who/what is blocking.
        current = conn.execute(
            text("SELECT owner_id, generation, expires_at FROM research_leases WHERE name = :name"),
            {"name": lease_name},
        ).fetchone()

    if current is None:
        # Should not happen (the INSERT would have applied), but don't
        # fabricate details if it somehow does.
        raise LeaseHeld(lease_name, None, None, None)
    raise LeaseHeld(lease_name, current[0], current[1], current[2])


def heartbeat(
    engine: Engine,
    lease_name: str,
    owner_id: str,
    generation: int,
    ttl_seconds: int = DEFAULT_LEASE_TTL_SECONDS,
) -> bool:
    """Renew the lease's expiry, IF this owner_id+generation is still the
    live, current row. Returns True if the row was renewed, False if this
    caller no longer owns a live lease (already expired or superseded) --
    the caller should treat False as "I am an orphan; stop trying to
    extend my ownership" rather than retry.
    """
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(seconds=ttl_seconds)
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                UPDATE research_leases
                SET heartbeat_at = :now, expires_at = :expires_at
                WHERE name = :name AND owner_id = :owner AND generation = :gen
                      AND expires_at > :now
                RETURNING generation
                """
            ),
            {"name": lease_name, "owner": owner_id, "gen": generation, "now": now, "expires_at": expires_at},
        ).fetchone()
    ok = row is not None
    if not ok:
        log.warning(
            "Heartbeat for lease {n} owner={o} generation={g} did NOT renew "
            "(already expired or superseded)", n=lease_name, o=owner_id, g=generation,
        )
    return ok


def release(engine: Engine, lease_name: str, owner_id: str, generation: int) -> bool:
    """Voluntarily give up the lease early (instead of waiting out its TTL).

    Sets expires_at to now (rather than deleting the row) so the NEXT
    acquire() takes the normal "expired" UPDATE path and keeps incrementing
    generation monotonically. Returns True if this owner+generation was
    actually the live row (and is now expired), False otherwise -- e.g. if
    this caller had already been superseded, in which case there is
    nothing to release.
    """
    now = datetime.now(timezone.utc)
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                UPDATE research_leases
                SET expires_at = :now
                WHERE name = :name AND owner_id = :owner AND generation = :gen
                RETURNING generation
                """
            ),
            {"name": lease_name, "owner": owner_id, "gen": generation, "now": now},
        ).fetchone()
    return row is not None


# ── Guarded writes ───────────────────────────────────────────────────────

def guarded_write(conn: Connection, lease_name: str, generation: int, fn: Callable[[Connection], T]) -> T:
    """SQLAlchemy-Connection-flavored guarded write.

    ``conn`` MUST already be inside an open transaction the caller controls
    (e.g. ``with engine.begin() as conn: guarded_write(conn, ...)``) --
    the row lock this takes is only meaningful for the lifetime of that
    transaction; if `conn` is autocommitting per-statement there is no
    window in which the lock actually protects `fn`.

    See the module docstring for why locking the row FIRST, then checking,
    then running `fn` in the SAME transaction is what makes "commits after
    loss" impossible, not merely unlikely.
    """
    row = conn.execute(
        text("SELECT generation, expires_at FROM research_leases WHERE name = :name FOR UPDATE"),
        {"name": lease_name},
    ).fetchone()

    if row is None:
        raise OwnershipLost(lease_name, generation, None)

    current_generation, expires_at = row[0], row[1]
    now = datetime.now(timezone.utc)
    expired = expires_at is None or expires_at <= now
    if current_generation != generation or expired:
        raise OwnershipLost(lease_name, generation, current_generation)

    return fn(conn)


def run_guarded(engine: Engine, lease_name: str, generation: int, fn: Callable[[Connection], T]) -> T:
    """Open a transaction on ``engine`` and run :func:`guarded_write` inside
    it. This is the usual entry point for SQLAlchemy callers (e.g. the
    ``write_guard`` hook passed into ``validation.backtest.WalkForwardBacktest``).
    """
    with engine.begin() as conn:
        return guarded_write(conn, lease_name, generation, fn)


def guarded_write_dbapi(cur: Any, lease_name: str, generation: int, fn: Callable[[Any], T]) -> T:
    """psycopg2-cursor-flavored guarded write (same ordering guarantee as
    :func:`guarded_write`, %s-style params).

    ``cur``'s connection MUST NOT be in autocommit mode for the duration of
    this call -- the lock, the check, and `fn`'s own statements must share
    one transaction, exactly as in the SQLAlchemy flavor. Callers normally
    reach this through :func:`run_guarded_dbapi`, which manages that.
    """
    cur.execute("SELECT generation, expires_at FROM research_leases WHERE name = %s FOR UPDATE", (lease_name,))
    row = cur.fetchone()

    if row is None:
        raise OwnershipLost(lease_name, generation, None)

    current_generation, expires_at = row[0], row[1]
    now = datetime.now(timezone.utc)
    expired = expires_at is None or expires_at <= now
    if current_generation != generation or expired:
        raise OwnershipLost(lease_name, generation, current_generation)

    return fn(cur)


def run_guarded_dbapi(pg_conn: Any, lease_name: str, generation: int, fn: Callable[[Any], T]) -> T:
    """Manage the transaction for a raw psycopg2 connection around
    :func:`guarded_write_dbapi`: temporarily turns autocommit off (restoring
    it afterward regardless of outcome), runs the guarded check + `fn` on a
    fresh cursor from `pg_conn`, and commits on success. On ANY exception
    (``OwnershipLost`` or otherwise) the transaction is rolled back before
    re-raising, so a caller catching ``OwnershipLost`` can rely on nothing
    from `fn` having been persisted.

    This is the entry point scripts/autoresearch.py uses for its raw
    psycopg2 writes (hypothesis_registry, model_registry) -- see
    `_guarded_or_direct` there.
    """
    prev_autocommit = pg_conn.autocommit
    if prev_autocommit:
        pg_conn.autocommit = False
    try:
        result = guarded_write_dbapi(pg_conn.cursor(), lease_name, generation, fn)
        pg_conn.commit()
        return result
    except Exception:
        pg_conn.rollback()
        raise
    finally:
        pg_conn.autocommit = prev_autocommit
