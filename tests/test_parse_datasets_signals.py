"""
Tests for ``scripts/parse_datasets.py``'s ``DatasetParser._insert_signals_batch``.

Background
----------
The ``INSERT INTO signal_data`` statement used to cast the ``data`` bind
parameter with the inline PostgreSQL cast operator: ``:data::jsonb``. Because
``::`` sits directly next to the ``:data`` token, SQLAlchemy's ``text()``
bind-parameter compiler does not recognize ``:data`` as a named parameter
there — it leaves the entire ``:data::jsonb`` token as literal SQL while
correctly compiling every other ``:name`` parameter. This produced a
``psycopg2.errors.SyntaxError: syntax error at or near ":"`` on every call.

Because ``_insert_signals_batch`` wraps the insert in a bare
``except Exception as e: log.error(...)`` with no re-raise, that syntax error
was silently swallowed — the entire batch of signal rows failed to insert,
every time, with no exception ever surfacing to the caller. This dropped all
236 ``congressional_large_trade`` signal rows during the 2026-09-13 data
reload.

The fix mirrors the pattern already used (correctly) elsewhere in this file
for casting jsonb bind parameters: ``CAST(:data AS jsonb)`` instead of
``:data::jsonb``.

This file proves two things:

1. ``test_insert_signals_batch_executes_against_real_postgres`` — the SQL
   actually executes without a syntax error, against the real ``griddb``
   Postgres instance, inside a transaction that is always rolled back (never
   committed). Skipped automatically if that database is not reachable.
2. ``test_insert_signals_batch_uses_cast_not_inline_jsonb_operator`` — a fast,
   DB-free regression guard asserting the broken ``:data::jsonb`` substring is
   absent and the safe ``CAST(:data AS jsonb)`` form is present in the
   method's source, so a future edit can't silently reintroduce the bug
   without a fast test catching it.
"""
from __future__ import annotations

import inspect
import json
import uuid
from contextlib import contextmanager
from datetime import date

import pytest
from sqlalchemy import text

from scripts.parse_datasets import DatasetParser


# ---------------------------------------------------------------------------
# Fast, DB-free regression guard
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_insert_signals_batch_uses_cast_not_inline_jsonb_operator():
    """``_insert_signals_batch`` must use ``CAST(:data AS jsonb)``, never
    ``:data::jsonb`` — the latter breaks SQLAlchemy's ``text()`` bind
    parameter compiler and silently drops every signal row (see module
    docstring for the full 2026-09-13 incident)."""
    source = inspect.getsource(DatasetParser._insert_signals_batch)

    assert ":data::jsonb" not in source, (
        "_insert_signals_batch regressed to the broken inline `:data::jsonb` "
        "cast. SQLAlchemy's text() compiler will not bind `:data` as a "
        "parameter when it is immediately followed by the `::` cast "
        "operator, so every insert will silently fail with a psycopg2 "
        "syntax error (caught and swallowed by the bare except below it)."
    )
    assert "CAST(:data AS jsonb)" in source, (
        "_insert_signals_batch must cast the data column with "
        "CAST(:data AS jsonb) so SQLAlchemy correctly binds :data as a "
        "parameter instead of treating it as literal SQL."
    )


@pytest.mark.unit
def test_insert_signals_batch_still_binds_all_other_parameters():
    """Sanity check that the fix didn't touch the other bind parameters."""
    source = inspect.getsource(DatasetParser._insert_signals_batch)
    for param in (
        ":signal_type",
        ":signal_date",
        ":ticker",
        ":actor",
        ":direction",
        ":magnitude",
        ":description",
        ":confidence",
        ":source_id",
    ):
        assert param in source, f"expected bind parameter {param} in INSERT"


# ---------------------------------------------------------------------------
# Live-DB check — proves the SQL actually executes against real Postgres
# ---------------------------------------------------------------------------
class _SavepointEngine:
    """Adapts an already-open SQLAlchemy ``Connection`` (with an active outer
    transaction) so code written against ``Engine.begin()`` — i.e.
    ``with self.engine.begin() as conn: conn.execute(...)`` — runs inside a
    ``SAVEPOINT`` nested in that outer transaction instead of opening a brand
    new connection/transaction of its own.

    This lets the test call the *real* ``_insert_signals_batch`` method
    against the real database while guaranteeing the outer transaction
    (which the test always rolls back, never commits) is the only thing that
    can ever land in the database — nothing this test does can leak a row
    into ``signal_data``.
    """

    def __init__(self, conn):
        self._conn = conn

    @contextmanager
    def begin(self):
        nested = self._conn.begin_nested()
        try:
            yield self._conn
        except Exception:
            nested.rollback()
            raise
        else:
            nested.commit()  # releases the SAVEPOINT only; outer txn untouched


@pytest.fixture
def griddb_conn():
    """Yield a live ``griddb`` connection wrapped in a transaction that is
    always rolled back, never committed. Skips the test if the real
    Postgres instance (as configured by ``config.settings`` / ``.env``) is
    unreachable.
    """
    from db import get_engine

    try:
        engine = get_engine()
        conn = engine.connect()
    except Exception as exc:
        pytest.skip(f"griddb not reachable: {exc}")

    outer_txn = conn.begin()
    try:
        yield conn
    finally:
        outer_txn.rollback()
        conn.close()


@pytest.mark.integration
def test_insert_signals_batch_executes_against_real_postgres(griddb_conn):
    """``_insert_signals_batch`` must execute against real Postgres without a
    syntax error.

    Runs entirely inside a transaction that is rolled back at the end of the
    test (see the ``griddb_conn`` fixture) — nothing is ever committed to
    ``signal_data``.

    ``_insert_signals_batch`` swallows exceptions internally (a bare
    ``except Exception: log.error(...)`` with no re-raise), so a syntax
    error would *not* raise here — it would just silently drop the batch.
    The only reliable proof that the INSERT actually executed is to read the
    row back within the same (uncommitted) transaction.
    """
    marker = f"pytest-signals-batch-{uuid.uuid4()}"

    dp = DatasetParser.__new__(DatasetParser)  # bypass __init__/_ensure_tables
    dp.engine = _SavepointEngine(griddb_conn)
    dp.datasets_dir = "/tmp"

    batch = [
        {
            "signal_type": "unit_test_signal",
            "signal_date": date(2026, 1, 1),
            "ticker": "TEST",
            "actor": "pytest",
            "direction": "BULL",
            "magnitude": 1.0,
            "description": "pytest verification row - rolled back, never committed",
            "data": json.dumps({"marker": marker, "source": "pytest"}),
            "confidence": "derived",
            "source_id": marker,
        }
    ]

    dp._insert_signals_batch(batch)

    row = griddb_conn.execute(
        text("SELECT source_id, data FROM signal_data WHERE source_id = :sid"),
        {"sid": marker},
    ).fetchone()

    assert row is not None, (
        "signal row was not found after _insert_signals_batch — the INSERT "
        "likely failed with a syntax error that was silently swallowed "
        "(regression of the :data::jsonb cast bug)"
    )
    assert row.source_id == marker
    assert row.data == {"marker": marker, "source": "pytest"}


@pytest.mark.integration
def test_insert_signals_batch_empty_batch_is_a_noop(griddb_conn):
    """Edge case: an empty batch must not attempt an INSERT at all."""
    dp = DatasetParser.__new__(DatasetParser)
    dp.engine = _SavepointEngine(griddb_conn)
    dp.datasets_dir = "/tmp"

    # Must not raise and must not touch the database.
    dp._insert_signals_batch([])
