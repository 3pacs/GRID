"""DB-gated tests for the issuer buyback quiet-window God View pillar.

Depends on ``godview_pg_engine`` (tests/godview/conftest.py) — skips with
"GRID_TEST_DB_URL not set" until the scratch-DB URL arrives.

``earnings_calendar`` is a lazily-created, untracked table
(``ingestion/altdata/earnings_calendar.py::_ensure_earnings_table``) --
these tests create it themselves with the exact DDL that module uses,
mirroring how a long-lived app instance would already have it, rather
than importing that module (which pulls in network-touching pieces this
test doesn't need).
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import text

from godview.buyback_pillar import (
    WINDOW_AFTER_DAYS,
    WINDOW_BEFORE_DAYS,
    materialize_buyback_pillar,
    read_buyback_pillar,
)

pytestmark = pytest.mark.integration


def _ensure_earnings_calendar_table(conn) -> None:
    conn.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS earnings_calendar (
                id SERIAL PRIMARY KEY,
                ticker TEXT NOT NULL,
                earnings_date DATE NOT NULL,
                fiscal_quarter TEXT,
                eps_estimate DOUBLE PRECISION,
                eps_actual DOUBLE PRECISION,
                eps_surprise_pct DOUBLE PRECISION,
                revenue_estimate DOUBLE PRECISION,
                revenue_actual DOUBLE PRECISION,
                revenue_surprise_pct DOUBLE PRECISION,
                classification TEXT DEFAULT 'pending',
                reported BOOLEAN DEFAULT FALSE,
                pull_timestamp TIMESTAMPTZ DEFAULT NOW(),
                raw_payload JSONB,
                UNIQUE (ticker, earnings_date)
            )
            """
        )
    )


def _insert_earnings_row(conn, ticker: str, earnings_date: date, *, pull_timestamp: datetime) -> None:
    conn.execute(
        text(
            "INSERT INTO earnings_calendar (ticker, earnings_date, pull_timestamp) "
            "VALUES (:t, :d, :pts) ON CONFLICT (ticker, earnings_date) DO NOTHING"
        ),
        {"t": ticker, "d": earnings_date, "pts": pull_timestamp},
    )


def test_migration_created_the_issuer_window_table(godview_pg_engine):
    with godview_pg_engine.begin() as conn:
        exists = conn.execute(text("SELECT to_regclass('issuer_buyback_blackout_windows')")).scalar()
    assert exists == "issuer_buyback_blackout_windows"


def test_materializer_writes_a_window_around_a_known_earnings_date(godview_pg_engine):
    engine = godview_pg_engine
    ticker = f"T{uuid.uuid4().hex[:8].upper()}"
    earnings_date = date(2026, 10, 20)

    with engine.begin() as conn:
        _ensure_earnings_calendar_table(conn)
        _insert_earnings_row(conn, ticker, earnings_date, pull_timestamp=datetime.combine(earnings_date, datetime.min.time()) - timedelta(days=30))

    result = materialize_buyback_pillar(engine, as_of=earnings_date)
    assert result.status == "SUCCESS"
    assert result.rows_written == WINDOW_BEFORE_DAYS + WINDOW_AFTER_DAYS + 1

    with engine.begin() as conn:
        row = conn.execute(
            text(
                "SELECT window_status, provenance, availability_basis, earnings_date_used "
                "FROM issuer_buyback_blackout_windows WHERE ticker = :t AND calendar_date = :d"
            ),
            {"t": ticker, "d": earnings_date},
        ).mappings().fetchone()
    assert row is not None
    assert row["window_status"] == "quiet_window"
    assert row["provenance"] == "modeled"
    assert row["availability_basis"] == "unknown"
    assert row["earnings_date_used"] == earnings_date


def test_issuer_with_no_earnings_date_has_no_window(godview_pg_engine):
    engine = godview_pg_engine
    known_ticker = f"K{uuid.uuid4().hex[:8].upper()}"
    unknown_ticker = f"U{uuid.uuid4().hex[:8].upper()}"
    earnings_date = date(2026, 11, 5)

    with engine.begin() as conn:
        _ensure_earnings_calendar_table(conn)
        _insert_earnings_row(conn, known_ticker, earnings_date, pull_timestamp=datetime.combine(earnings_date, datetime.min.time()) - timedelta(days=30))

    result = materialize_buyback_pillar(engine, as_of=earnings_date)
    assert result.status == "SUCCESS"

    with engine.begin() as conn:
        read = read_buyback_pillar(conn, earnings_date)
    tickers_with_windows = {r["ticker"] for r in read.rows}
    assert known_ticker in tickers_with_windows
    assert unknown_ticker not in tickers_with_windows


def test_never_configured_without_earnings_calendar_table(godview_pg_engine):
    with godview_pg_engine.begin() as conn:
        result = read_buyback_pillar(conn, date.today())
    assert result.state in ("never_configured", "materializer_failed", "ok")
