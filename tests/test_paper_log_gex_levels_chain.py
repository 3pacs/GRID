"""Tests for the PIT-safe chain-selection query.

Uses an in-memory SQLite engine as a local, dependency-free SQL sandbox —
no network, no external database server, no credentials — purely to
exercise the actual GROUP BY / HAVING / ORDER BY semantics of the query in
``chain.py`` (a mocked connection would only prove this module wraps
*whatever a connection returns*, not that the SQL is PIT-safe, which is
the property that actually matters here). The query is portable ANSI SQL
(no Postgres-specific syntax), and result columns are given explicit
``Date``/``DateTime`` types via ``.columns(...)`` in ``chain.py`` so
values round-trip as real ``date``/``datetime`` objects exactly as
psycopg2 would return in production — verified directly, not assumed.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest
import sqlalchemy as sa

from paper_log.gex_levels.chain import select_chain_snapshot


@pytest.fixture
def sqlite_engine():
    engine = sa.create_engine("sqlite:///:memory:")
    meta = sa.MetaData()
    sa.Table(
        "options_snapshots", meta,
        sa.Column("ticker", sa.Text),
        sa.Column("snap_date", sa.Date),
        sa.Column("created_at", sa.DateTime(timezone=True)),
    )
    meta.create_all(engine)
    try:
        yield engine
    finally:
        engine.dispose()


def _insert(engine, rows: list[dict]) -> None:
    with engine.begin() as conn:
        conn.execute(sa.text(
            "INSERT INTO options_snapshots (ticker, snap_date, created_at) "
            "VALUES (:ticker, :snap_date, :created_at)"
        ), rows)


def test_no_chain_when_table_empty(sqlite_engine) -> None:
    run_at = datetime(2026, 9, 24, 13, 0, tzinfo=timezone.utc)
    assert select_chain_snapshot(sqlite_engine, "SPY", run_at) is None


def test_picks_latest_snap_date_whose_batch_fully_predates_run(sqlite_engine) -> None:
    _insert(sqlite_engine, [
        {"ticker": "SPY", "snap_date": date(2026, 9, 22), "created_at": datetime(2026, 9, 22, 20, 0, tzinfo=timezone.utc)},
        {"ticker": "SPY", "snap_date": date(2026, 9, 23), "created_at": datetime(2026, 9, 23, 20, 0, tzinfo=timezone.utc)},
    ])
    run_at = datetime(2026, 9, 24, 13, 0, tzinfo=timezone.utc)
    result = select_chain_snapshot(sqlite_engine, "SPY", run_at)
    assert result is not None
    assert result.snap_date == date(2026, 9, 23)
    assert result.created_at == datetime(2026, 9, 23, 20, 0, tzinfo=timezone.utc)


def test_excludes_a_batch_whose_rows_are_still_being_written(sqlite_engine) -> None:
    """The PIT-critical case: today's snap_date has SOME rows written
    before run_at and SOME after (the ingestion job is still running). The
    whole batch must be treated as not-yet-available, falling back to
    yesterday's complete batch — never a partial read of today's."""
    _insert(sqlite_engine, [
        {"ticker": "SPY", "snap_date": date(2026, 9, 23), "created_at": datetime(2026, 9, 23, 20, 0, tzinfo=timezone.utc)},
        {"ticker": "SPY", "snap_date": date(2026, 9, 24), "created_at": datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc)},
        {"ticker": "SPY", "snap_date": date(2026, 9, 24), "created_at": datetime(2026, 9, 24, 8, 50, tzinfo=timezone.utc)},  # after run_at
    ])
    run_at = datetime(2026, 9, 24, 8, 45, tzinfo=timezone.utc)
    result = select_chain_snapshot(sqlite_engine, "SPY", run_at)
    assert result is not None
    assert result.snap_date == date(2026, 9, 23)  # NOT the 24th — its batch isn't complete yet


def test_includes_a_batch_whose_rows_all_predate_run(sqlite_engine) -> None:
    _insert(sqlite_engine, [
        {"ticker": "SPY", "snap_date": date(2026, 9, 24), "created_at": datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc)},
        {"ticker": "SPY", "snap_date": date(2026, 9, 24), "created_at": datetime(2026, 9, 24, 8, 10, tzinfo=timezone.utc)},
    ])
    run_at = datetime(2026, 9, 24, 8, 45, tzinfo=timezone.utc)
    result = select_chain_snapshot(sqlite_engine, "SPY", run_at)
    assert result is not None
    assert result.snap_date == date(2026, 9, 24)
    assert result.created_at == datetime(2026, 9, 24, 8, 10, tzinfo=timezone.utc)


def test_filters_by_ticker(sqlite_engine) -> None:
    _insert(sqlite_engine, [
        {"ticker": "QQQ", "snap_date": date(2026, 9, 24), "created_at": datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc)},
    ])
    run_at = datetime(2026, 9, 24, 8, 45, tzinfo=timezone.utc)
    assert select_chain_snapshot(sqlite_engine, "SPY", run_at) is None


def test_no_chain_when_every_batch_is_still_in_progress(sqlite_engine) -> None:
    _insert(sqlite_engine, [
        {"ticker": "SPY", "snap_date": date(2026, 9, 24), "created_at": datetime(2026, 9, 24, 9, 0, tzinfo=timezone.utc)},
    ])
    run_at = datetime(2026, 9, 24, 8, 45, tzinfo=timezone.utc)
    assert select_chain_snapshot(sqlite_engine, "SPY", run_at) is None
