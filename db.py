"""
GRID database access layer.

Provides SQLAlchemy engine creation, raw psycopg2 connection management,
schema application, health checks, and simple query execution utilities.
All connection events are logged via loguru.
"""

from __future__ import annotations

import contextlib
import os
import sys
import threading
import time
from pathlib import Path
from typing import Any, Generator

import psycopg2
import psycopg2.extras
from loguru import logger as log
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine

from config import settings


# Permanent-looking psycopg2 OperationalError substrings that indicate the
# DB server is out of connection slots. These are transient (retry-able with
# short backoff) rather than permanent.
_CONN_SLOT_EXHAUSTION_MARKERS = (
    "remaining connection slots are reserved",
    "too many clients already",
    "sorry, too many clients",
)


def _is_slot_exhaustion_error(exc: BaseException) -> bool:
    """True iff exc looks like a postgres connection-slot exhaustion."""
    msg = str(exc).lower()
    return any(m in msg for m in _CONN_SLOT_EXHAUSTION_MARKERS)


# ---------------------------------------------------------------------------
# SQLAlchemy Engine (singleton)
# ---------------------------------------------------------------------------
_engine: Engine | None = None

# Thread-safe high-water-mark for checked-out connections. A per-cycle or
# per-health-check point sample can land between bursts and miss them
# entirely; this records the peak seen by any thread since the last reset,
# via the pool's own checkout event, so short-lived spikes are never lost
# between observations.
_pool_peak_checked_out = 0
_pool_peak_lock = threading.Lock()


def _record_checkout_peak(checked_out: int) -> None:
    """Update the thread-safe checked-out high-water-mark if exceeded."""
    global _pool_peak_checked_out
    with _pool_peak_lock:
        if checked_out > _pool_peak_checked_out:
            _pool_peak_checked_out = checked_out


def get_pool_peak_checked_out() -> int:
    """Return the highest checked-out count observed since the last reset."""
    with _pool_peak_lock:
        return _pool_peak_checked_out


# ---------------------------------------------------------------------------
# Checkout attribution — WHICH thread holds each open connection and for how
# long, so a burst like the one on 2026-09-17 (checked_out climbing 17->48
# during a hermes cycle) can be attributed by thread/lifetime instead of
# re-investigated from scratch each time. Deliberately does NOT attempt to
# capture the calling code's stack (SQLAlchemy pool internals are between
# the checkout event and any application frame, so a cheap single-frame
# lookup would only show pool-internal code, and a full stack walk on every
# checkout is not bounded overhead on a hot path). Thread name is enough to
# distinguish concurrent owners sharing one process's pool (e.g. a cycle
# worker thread vs. the llm-taskqueue background thread) and correlates
# against each thread's own structured logs (cycle/task boundaries) for
# finer attribution. Logs no SQL, no query parameters, no connection
# strings, and no credentials -- only thread names, counts, and durations.
#
# Lifecycle notes (verified by test, not assumed):
# - Keyed by id(connection_rec), the pool's own per-slot identity object --
#   stable across pool_pre_ping-triggered reconnects and invalidation
#   within the same checkout, since those are handled inside a single
#   checkout attempt before the checkout event fires for the connection
#   actually handed out. A retried/invalidated attempt during that same
#   checkout never gets its own checkout event, so it can't leave a
#   duplicate or orphaned entry here.
# - checkin fires (and is tracked as released) even when the connection
#   itself was invalidated mid-use -- invalidation replaces the underlying
#   DBAPI connection for next time; it does not skip returning the pool
#   slot via checkin. Confirmed by test, not assumed.
# - Checkout and checkin are not required to happen on the same thread
#   (e.g. a connection object passed between threads, or a framework
#   detail closing it elsewhere). The acquiring thread is what matters for
#   attributing *why* a connection is open, so it is captured at checkout
#   time and preserved through to checkin regardless of which thread
#   actually releases it; the releasing thread is captured separately.
# - get_engine()'s singleton can be replaced via clear_engine() (e.g. a
#   runtime config change). Any checkouts still open against the disposed
#   engine would otherwise never receive a matching checkin from the new
#   engine's listeners and become permanently stale entries; clear_engine()
#   now clears this table (and the peak tracker) for that reason.
# ---------------------------------------------------------------------------
_checkout_lock = threading.Lock()
# id(connection_rec) -> (acquiring_thread_name, monotonic_open_time)
_open_checkouts: dict[int, tuple[str, float]] = {}

# A connection held open this long is notable regardless of pool pressure --
# most queries in this codebase are expected to complete in well under a
# second (db.py's own statement_timeout defaults to 120s as a hard ceiling,
# but that bounds a single statement, not how long application code might
# hold a connection open across several statements or other work).
_LIFETIME_WARN_SECONDS = 5.0

# Defensive cap on how many individual open-checkout entries
# get_outstanding_checkouts() returns in one call. Pool capacity in this
# codebase is bounded to roughly 20-50 connections per process by
# GRID_DB_POOL_SIZE/GRID_DB_MAX_OVERFLOW, so this is not expected to bind
# in practice -- it exists so a future misconfiguration can't turn this
# into unbounded logging.
_MAX_REPORTED_OPEN_CHECKOUTS = 200


def _record_checkout_open(connection_rec: object) -> None:
    """Record that a connection was just checked out, by whichever thread did it."""
    thread_name = threading.current_thread().name
    with _checkout_lock:
        _open_checkouts[id(connection_rec)] = (thread_name, time.monotonic())


def _record_checkout_closed(connection_rec: object) -> str | None:
    """Record that a connection was returned; warn if held unusually long.

    Returns:
        The thread name that originally *acquired* this connection, or
        None if it wasn't tracked (e.g. checked out before this process's
        engine was (re)created). The thread calling this function now --
        the one *releasing* it -- may be a different thread; both are
        distinguished in the warning message rather than conflated.
    """
    key = id(connection_rec)
    with _checkout_lock:
        entry = _open_checkouts.pop(key, None)
    if entry is None:
        return None
    acquired_by, opened_at = entry
    lifetime_s = time.monotonic() - opened_at
    released_by = threading.current_thread().name
    if lifetime_s > _LIFETIME_WARN_SECONDS:
        log.warning(
            "DB connection held {s:.1f}s — acquired_by={a}, released_by={r}",
            s=lifetime_s, a=acquired_by, r=released_by,
        )
    return acquired_by


def get_checkout_attribution() -> dict[str, Any]:
    """Return currently-open checkouts grouped by acquiring thread, with ages.

    Returns:
        dict with ``by_thread`` (acquiring thread name -> count of
        connections currently open that it acquired), ``oldest_open_seconds``
        (age of the longest-held currently-open connection, or None if none
        are open), and ``open_count`` (total open connections tracked).
        Contains no SQL, parameters, or connection details -- only thread
        names, counts, and durations.
    """
    now = time.monotonic()
    with _checkout_lock:
        entries = list(_open_checkouts.values())
    by_thread: dict[str, int] = {}
    oldest_age: float | None = None
    for thread_name, opened_at in entries:
        by_thread[thread_name] = by_thread.get(thread_name, 0) + 1
        age = now - opened_at
        if oldest_age is None or age > oldest_age:
            oldest_age = age
    return {"by_thread": by_thread, "oldest_open_seconds": oldest_age, "open_count": len(entries)}


def get_outstanding_checkouts(min_age_seconds: float = 0.0) -> list[dict[str, Any]]:
    """Bounded, point-in-time visibility into every currently-open connection.

    A checkin-side "held too long" warning (see :func:`_record_checkout_closed`)
    can only fire once a connection actually returns -- a connection that
    never returns at all (leaked, or held for the remainder of the process)
    would never trigger it. Call this periodically (e.g. alongside
    get_pool_stats() in a per-cycle log) to catch that case instead: it
    lists every currently-open connection's acquiring thread and current
    age, oldest first, filtered to at least ``min_age_seconds`` and capped
    at :data:`_MAX_REPORTED_OPEN_CHECKOUTS` entries. No SQL, parameters, or
    connection details -- only thread name and age.
    """
    now = time.monotonic()
    with _checkout_lock:
        entries = list(_open_checkouts.values())
    result = [
        {"acquired_by": thread_name, "age_seconds": round(now - opened_at, 1)}
        for thread_name, opened_at in entries
        if (now - opened_at) >= min_age_seconds
    ]
    result.sort(key=lambda r: r["age_seconds"], reverse=True)
    return result[:_MAX_REPORTED_OPEN_CHECKOUTS]


def reset_pool_peak(baseline: int = 0) -> None:
    """Reset the checked-out high-water-mark, using ``baseline`` as the floor.

    Connections already checked out at the moment of reset won't fire a
    new "checkout" event — they were borrowed before the reset and stay
    borrowed — so a bare reset to 0 would silently under-report the next
    interval's true peak until *another* checkout happens to occur. Pass
    the pool's current ``checked_out`` count (e.g. from
    :func:`get_pool_stats`) as ``baseline`` so the next interval starts
    from what is actually outstanding right now, not from zero.

    Call this after reading :func:`get_pool_peak_checked_out` so the next
    read reflects only the interval since the reset, not the whole
    process's lifetime.
    """
    global _pool_peak_checked_out
    with _pool_peak_lock:
        _pool_peak_checked_out = baseline


def get_engine() -> Engine:
    """Return the SQLAlchemy engine, creating it on first call.

    The engine is configured with a connection pool of 5 connections,
    up to 10 overflow connections, and a 30-second timeout.

    Returns:
        sqlalchemy.engine.Engine: Configured engine instance.
    """
    global _engine
    if _engine is None:
        # Default budget lowered from 50+100=150 → 20+30=50 on 2026-04-19:
        # postgres's default max_connections is 100 with ~3 slots reserved
        # for superuser. Leaving 150 in the SQLAlchemy pool alone could
        # (and did, as of today) exhaust slots shared with raw psycopg2
        # callers (candle flusher, ws_listener, events/producer, …).
        # Override via GRID_DB_POOL_SIZE / GRID_DB_MAX_OVERFLOW if postgres
        # is sized larger.
        pool_size = int(os.getenv("GRID_DB_POOL_SIZE", os.getenv("DB_POOL_SIZE", "20")))
        max_overflow = int(os.getenv("GRID_DB_MAX_OVERFLOW", os.getenv("DB_MAX_OVERFLOW", "30")))
        log.info("Creating SQLAlchemy engine — {url}", url=settings.DB_URL.replace(settings.DB_PASSWORD, "***"))
        # Default per-statement timeout (milliseconds). Any single SQL
        # statement that runs longer than this is killed by postgres
        # before it can exhaust the connection pool. Override per-call
        # with `SET LOCAL statement_timeout = 0` for jobs that legitimately
        # need longer (long backfills, bulk resolves, etc.).
        #
        # Why 120s: the actual runaway scan that took down the lever page
        # + NVDA chart + canvas on 2026-04-15 was 215s long, so 120s still
        # catches it. 120s also leaves a big safety margin for any code
        # path that interleaves an LLM call between SQL statements — the
        # timeout is per-statement, not per-connection, so an LLM call
        # BETWEEN two fast queries is unaffected (each execute() resets
        # the clock). We grepped the codebase for stream_results /
        # server_side_cursor / yield_per and found nothing in the request
        # path, so every SELECT fetches eagerly and the statement
        # completes before the LLM call starts. 120s is conservative
        # cushion for any pattern we missed.
        statement_timeout_ms = int(
            os.getenv("GRID_DB_STATEMENT_TIMEOUT_MS", "120000")  # 120s
        )
        _engine = create_engine(
            settings.DB_URL,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=30,
            pool_pre_ping=True,
            pool_recycle=3600,  # Invalidate stale connections after 1 hour
            connect_args={
                "options": f"-c statement_timeout={statement_timeout_ms}",
            },
        )

        # Pool utilization monitoring: warn when >80% of capacity is checked out.
        _warn_threshold = int(pool_size * 0.8)

        @event.listens_for(_engine, "checkout")
        def _on_checkout(dbapi_conn, connection_rec, connection_proxy):  # noqa: ARG001
            # A telemetry bug here must never break a real checkout:
            # SQLAlchemy event listeners run synchronously as part of the
            # checkout itself, so an uncaught exception in this handler
            # would propagate out of engine.connect() and fail application
            # code that has nothing to do with instrumentation. Every
            # instrumentation call is therefore its own try/except,
            # independent of the others, so one failing can't suppress or
            # skip the rest.
            #
            # Event-ordering / consistency note: SQLAlchemy fires
            # "checkout" AFTER the pool's internal counter is already
            # incremented for this connection, so pool.checkedout() read
            # here already reflects it -- this handler is not racing its
            # own checkout. The attribution map (_open_checkouts) is
            # updated under a *separate* lock from the pool's own internal
            # counter, so under concurrent checkouts on other threads there
            # is a narrow window where pool.checkedout() and
            # sum(get_checkout_attribution()['by_thread'].values()) can
            # transiently disagree by however many checkouts are mid-flight
            # through this same handler on other threads at that instant.
            # Both converge as soon as those handlers finish; the
            # disagreement does not persist or compound.
            checked_out = None
            capacity = None
            try:
                checked_out = _engine.pool.checkedout()  # type: ignore[union-attr]
                capacity = pool_size + max_overflow
                _record_checkout_peak(checked_out)
            except Exception as exc:
                log.debug("Pool peak tracking failed: {e}", e=str(exc))
            try:
                _record_checkout_open(connection_rec)
            except Exception as exc:
                log.debug("Checkout attribution tracking failed: {e}", e=str(exc))
            if checked_out is not None and checked_out > _warn_threshold:
                try:
                    log.warning(
                        "DB pool utilization high — {co}/{cap} connections checked out ({pct:.0f}%) "
                        "owners={owners}",
                        co=checked_out,
                        cap=capacity,
                        pct=checked_out / capacity * 100,
                        owners=get_checkout_attribution()["by_thread"],
                    )
                except Exception as exc:
                    log.debug("Pool utilization warning logging failed: {e}", e=str(exc))

        @event.listens_for(_engine, "checkin")
        def _on_checkin(dbapi_conn, connection_rec):  # noqa: ARG001
            # Same isolation guarantee as _on_checkout above: this must
            # never prevent a connection from actually being returned to
            # the pool.
            try:
                _record_checkout_closed(connection_rec)
            except Exception as exc:
                log.debug("Checkout attribution checkin tracking failed: {e}", e=str(exc))

        log.info(
            "SQLAlchemy engine created — pool_size={ps}, max_overflow={mo}",
            ps=pool_size, mo=max_overflow,
        )
    return _engine


def clear_engine() -> None:
    """Dispose the cached SQLAlchemy engine and clear the singleton.

    The next call to :func:`get_engine` will build a fresh engine from
    current ``settings``. Safe to call when no engine has been created.

    Also clears the checkout-attribution table and peak tracker. Any
    checkouts still open against the disposed engine belong to a pool that
    no longer exists — the new engine created by the next get_engine() call
    registers its own fresh listeners and would never see a matching
    checkin for those old entries, leaving them stale forever otherwise.
    """
    global _engine
    if _engine is not None:
        _engine.dispose()
    _engine = None
    with _checkout_lock:
        _open_checkouts.clear()
    reset_pool_peak()


def get_pool_stats(engine: Engine | None = None) -> dict[str, int]:
    """Return this process's actual SQLAlchemy pool instrumentation.

    Distinguishes configured capacity from live usage so callers (health
    endpoints, per-cycle log lines) never have to infer checked-out
    concurrency from ``pg_stat_activity.state`` — that only shows whether a
    backend is running a query *right now*, not how many connections this
    process is holding checked out of its own pool at all.

    Returns:
        dict with ``pool_size`` (configured retained capacity),
        ``max_overflow`` (configured burst capacity beyond pool_size),
        ``checked_in`` (idle, available in the pool right now),
        ``checked_out`` (borrowed by in-flight work right now),
        ``peak_checked_out`` (thread-safe high-water-mark of checked_out
        since the last :func:`reset_pool_peak` call — catches bursts a
        point-in-time sample would miss between observations),
        ``overflow`` (SQLAlchemy's raw overflow counter — negative means
        the pool holds fewer live connections than pool_size),
        ``capacity`` (pool_size + max_overflow, the hard ceiling), and
        ``checkout_owners`` (currently-open checkouts grouped by thread
        name, plus the oldest open connection's age — see
        :func:`get_checkout_attribution`).
    """
    eng = engine if engine is not None else get_engine()
    pool = eng.pool
    pool_size = pool.size()  # type: ignore[attr-defined]
    max_overflow = getattr(pool, "_max_overflow", 0)
    checked_out = pool.checkedout()  # type: ignore[attr-defined]
    return {
        "pool_size": pool_size,
        "max_overflow": max_overflow,
        "checked_in": pool.checkedin(),  # type: ignore[attr-defined]
        "checked_out": checked_out,
        "peak_checked_out": get_pool_peak_checked_out(),
        "overflow": pool.overflow(),  # type: ignore[attr-defined]
        "capacity": pool_size + max_overflow,
        "checkout_owners": get_checkout_attribution(),
    }


def _connect_with_retry(max_attempts: int = 5) -> psycopg2.extensions.connection:
    """Open a raw psycopg2 connection with retry-on-slot-exhaustion.

    Postgres returns a transient FATAL when `max_connections` is hit; the
    slot usually frees within a few hundred ms. Bare `psycopg2.connect()`
    raises OperationalError immediately, which in our case crashes
    long-lived async workers (candle flusher, ws listeners). Retry with
    bounded exponential backoff up to max_attempts.
    """
    last_exc: BaseException | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return psycopg2.connect(
                host=settings.DB_HOST,
                port=settings.DB_PORT,
                dbname=settings.DB_NAME,
                user=settings.DB_USER,
                password=settings.DB_PASSWORD,
            )
        except psycopg2.OperationalError as exc:
            last_exc = exc
            if not _is_slot_exhaustion_error(exc) or attempt == max_attempts:
                raise
            # 0.5s, 1s, 2s, 4s — capped at 4s per sleep
            delay = min(0.5 * (2 ** (attempt - 1)), 4.0)
            log.warning(
                "DB slot-exhaustion on connect (attempt {a}/{m}); "
                "retrying in {d:.1f}s",
                a=attempt, m=max_attempts, d=delay,
            )
            time.sleep(delay)
    # Unreachable: loop either returns or raises, but keep mypy happy.
    assert last_exc is not None
    raise last_exc


@contextlib.contextmanager
def get_connection() -> Generator[psycopg2.extensions.connection, None, None]:
    """Yield a raw psycopg2 connection as a context manager.

    The connection is committed on clean exit and rolled back on exception.
    Always closed when the context exits.

    Yields:
        psycopg2.extensions.connection: Active database connection.

    Raises:
        psycopg2.OperationalError: If the database is unreachable after
            retries (including slot-exhaustion retries).
    """
    conn = None
    try:
        log.debug("Opening raw psycopg2 connection")
        conn = _connect_with_retry()
        yield conn
        conn.commit()
        log.debug("Connection committed")
    except Exception:
        if conn is not None:
            conn.rollback()
            log.warning("Connection rolled back due to exception")
        raise
    finally:
        if conn is not None:
            conn.close()
            log.debug("Connection closed")


def execute_sql(sql: str, params: tuple | dict | None = None) -> list[dict[str, Any]]:
    """Execute a SQL statement and return results as a list of dicts.

    Parameters:
        sql: SQL query string. May use %s or %(name)s placeholders.
        params: Optional parameters for the query.

    Returns:
        list[dict]: Rows as dictionaries. Empty list for non-SELECT queries.
    """
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            log.debug("Executing SQL: {sql}", sql=sql[:120])
            cur.execute(sql, params)
            if cur.description is not None:
                rows = [dict(row) for row in cur.fetchall()]
                log.debug("Query returned {n} rows", n=len(rows))
                return rows
            return []


def apply_schema(schema_path: str | None = None) -> None:
    """Read and execute schema.sql against the database.

    Parameters:
        schema_path: Path to the SQL schema file.  Defaults to
                     ``schema.sql`` in the same directory as this module.

    Raises:
        FileNotFoundError: If the schema file does not exist.
        psycopg2.Error: If the SQL is invalid.
    """
    if schema_path is None:
        schema_path = str(Path(__file__).parent / "schema.sql")

    path = Path(schema_path)
    if not path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    sql = path.read_text(encoding="utf-8")
    log.info("Applying schema from {path} ({size} bytes)", path=schema_path, size=len(sql))

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
    log.info("Schema applied successfully")


def health_check() -> bool:
    """Check whether the database is reachable.

    Returns:
        bool: True if a simple query succeeds, False otherwise.
    """
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log.info("Database health check passed")
        return True
    except Exception as exc:
        log.error("Database health check failed: {err}", err=str(exc))
        return False


# ---------------------------------------------------------------------------
# CLI entry point: apply schema when run directly
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    log.info("Running db.py — applying schema")
    apply_schema()
    if health_check():
        log.info("Database is ready")
    else:
        log.error("Database health check failed after schema application")
        sys.exit(1)
