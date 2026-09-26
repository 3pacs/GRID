"""Read-only database access for the GEX-levels paper log.

Deliberately independent of ``db.get_engine()`` (GRID's shared, pooled,
read-write singleton used by the API/Hermes). This job is a twice-daily
batch script that must never be able to write to the database, so it gets
its own small engine whose every physical connection is pinned read-only
and short-timeout at the moment it is opened — not per-checkout, not
per-transaction, so there is no code path (in this module, in
``physics.dealer_gamma``, or in a future bug) that can escape it for the
life of that connection.

Pre-registration: "The job opens database sessions read-only and places no
orders." Task spec (7): "create the engine so every connection runs `SET
default_transaction_read_only = on` and `SET statement_timeout = '20s'` on
connect (SQLAlchemy event listener); assert it with SHOW in the preopen
path."
"""

from __future__ import annotations

from sqlalchemy import event, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool
from loguru import logger as log

from config import settings
from paper_log.gex_levels.config import DB_STATEMENT_TIMEOUT


class ReadOnlyGuardError(RuntimeError):
    """Raised when a connection does not verify as read-only / timeout-bound."""


def _pin_connection_readonly(dbapi_connection) -> None:
    """The actual guard: run on every new physical DBAPI connection.

    Pulled out of the ``"connect"`` event closure so it can be unit-tested
    directly against a fake DBAPI connection (a `MagicMock`), independent
    of whichever database the engine itself is pointed at — Postgres's
    `SET default_transaction_read_only` / `SET statement_timeout` syntax
    isn't understood by, e.g., SQLite, so this can't be exercised through
    a real connection in a network/DB-free test.
    """
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("SET default_transaction_read_only = on")
        cursor.execute(f"SET statement_timeout = '{DB_STATEMENT_TIMEOUT}'")
        dbapi_connection.commit()
    finally:
        cursor.close()
    log.debug(
        "paper_log.gex_levels: new DB connection pinned read-only, "
        "statement_timeout={t}",
        t=DB_STATEMENT_TIMEOUT,
    )


def build_readonly_engine(db_url: str | None = None) -> Engine:
    """Build a dedicated read-only SQLAlchemy engine.

    ``NullPool`` is deliberate: this is a sparse batch job (twice a day),
    not a server. Using NullPool means every ``connect()`` call opens a
    fresh physical connection, so the ``"connect"`` event below —  which
    fires once per new DBAPI connection — reliably applies the guard every
    single time this engine is used, with no pooled connection ever able to
    skip it because it was opened before another caller subscribed.
    """
    url = db_url or settings.DB_URL
    engine = create_engine_readonly(url)

    @event.listens_for(engine, "connect")
    def _on_connect(dbapi_connection, connection_record) -> None:  # noqa: ARG001
        _pin_connection_readonly(dbapi_connection)

    return engine


def create_engine_readonly(url: str) -> Engine:
    """Thin wrapper so tests can patch engine creation without patching
    SQLAlchemy's ``create_engine`` globally."""
    from sqlalchemy import create_engine

    return create_engine(url, poolclass=NullPool, pool_pre_ping=False)


def assert_read_only(engine: Engine) -> None:
    """Verify a live connection is actually read-only / timeout-bound.

    Raises :class:`ReadOnlyGuardError` if either SHOW does not report the
    value the "connect" listener is supposed to have set. Called once, in
    the preopen path, before any query that touches ``options_snapshots``.
    """
    with engine.connect() as conn:
        read_only = conn.execute(text("SHOW default_transaction_read_only")).scalar()
        timeout = conn.execute(text("SHOW statement_timeout")).scalar()

    if str(read_only).strip().lower() != "on":
        raise ReadOnlyGuardError(
            f"expected default_transaction_read_only=on, got {read_only!r}"
        )
    if str(timeout).strip() != DB_STATEMENT_TIMEOUT:
        raise ReadOnlyGuardError(
            f"expected statement_timeout={DB_STATEMENT_TIMEOUT!r}, got {timeout!r}"
        )
    log.info(
        "paper_log.gex_levels: read-only DB guard verified "
        "(default_transaction_read_only=on, statement_timeout={t})",
        t=DB_STATEMENT_TIMEOUT,
    )
