"""Opt-in disposable PostgreSQL integration; never use application DB settings."""

import os
from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text

from analysis.options_capture_panel import load_capture_panel

DAY = date(2026, 11, 10)
END = datetime(2026, 11, 10, 22, 20, tzinfo=timezone.utc)
CUTOFF = END + timedelta(minutes=1)


@pytest.fixture
def db():
    url = os.environ.get("D2_PANEL_TEST_URL")
    if not url:
        pytest.skip("requires isolated D2_PANEL_TEST_URL")
    engine = create_engine(url)
    assert engine.url.database == "d2_test"
    assert str(engine.url.query.get("host", "")).startswith("/tmp/grid-d2-pg.")
    with engine.begin() as conn:
        assert conn.execute(
            text("SELECT current_database(), inet_server_addr()")
        ).one() == ("d2_test", None)
        conn.execute(
            text("""CREATE TABLE options_capture_batches (
            capture_batch_id TEXT PRIMARY KEY, ticker TEXT NOT NULL, snap_date DATE NOT NULL,
            capture_ordinal BIGINT NOT NULL, row_count INTEGER NOT NULL,
            capture_started_at TIMESTAMPTZ NOT NULL, capture_completed_at TIMESTAMPTZ NOT NULL,
            registered_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            spot_price DOUBLE PRECISION, spot_provider TEXT, capture_source TEXT NOT NULL,
            backfilled BOOLEAN NOT NULL DEFAULT false, provider_regular_market_at TIMESTAMPTZ)""")
        )
        conn.execute(
            text("""CREATE TABLE options_snapshots_all (
            capture_batch_id TEXT, capture_ordinal BIGINT, ticker TEXT, snap_date DATE,
            capture_started_at TIMESTAMPTZ, capture_completed_at TIMESTAMPTZ,
            provider_regular_market_at TIMESTAMPTZ, created_at TIMESTAMPTZ,
            expiry DATE, strike DOUBLE PRECISION, opt_type TEXT,
            open_interest INTEGER, implied_vol DOUBLE PRECISION, last_price DOUBLE PRECISION,
            bid DOUBLE PRECISION, ask DOUBLE PRECISION, volume INTEGER)""")
        )
    yield engine
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE options_snapshots_all, options_capture_batches"))
    engine.dispose()


def insert(conn, *, batch="A", registered=None, count=1, spot=750):
    params = dict(
        batch=batch,
        day=DAY,
        start=END - timedelta(seconds=20),
        end=END,
        registered=registered or END + timedelta(seconds=1),
        count=count,
        spot=spot,
        market=END - timedelta(hours=1),
        expiry=DAY + timedelta(days=7),
    )
    conn.execute(
        text("""INSERT INTO options_capture_batches VALUES
        (:batch,'SPY',:day,1,:count,:start,:end,:registered,:spot,'source','source',false,:market)"""),
        params,
    )
    conn.execute(
        text("""INSERT INTO options_snapshots_all VALUES
        (:batch,1,'SPY',:day,:start,:end,:market,:end,:expiry,750,'call',100,.2,10,9,11,20)"""),
        params,
    )


def read(db):
    return load_capture_panel(db, "SPY", "A", CUTOFF)


def test_postgres_exact_batch_returns_registry_spot_and_native_clocks(db):
    with db.begin() as c:
        insert(c)
        insert(c, batch="B", spot=999)
    result = read(db)
    assert result["diagnostic_complete"]
    assert result["capture"]["spot_price"] == 750
    assert result["capture"]["snap_date"] == DAY
    assert result["capture"]["completed_at"] == END
    assert result["contracts"][0]["row_batch_id"] == "A"
    assert not result["available"] and not result["research_eligible"]


def test_postgres_left_join_exposes_missing_contracts(db):
    with db.begin() as c:
        insert(c)
        c.execute(text("DELETE FROM options_snapshots_all"))
    assert read(db)["reasons"] == ["incomplete_capture"]


def test_postgres_count_mismatch_rejected(db):
    with db.begin() as c:
        insert(c, count=2)
    assert read(db)["reasons"] == ["incomplete_capture"]


def test_postgres_late_registration_rejected(db):
    with db.begin() as c:
        insert(c, registered=CUTOFF + timedelta(seconds=1))
    assert read(db)["reasons"] == ["invalid_or_future_source_clock"]


def test_postgres_mixed_identity_rejected(db):
    with db.begin() as c:
        insert(c)
        c.execute(text("UPDATE options_snapshots_all SET ticker='QQQ'"))
    assert read(db)["reasons"] == ["mixed_contract_identity"]


def test_postgres_nonfinite_spot_rejected(db):
    with db.begin() as c:
        insert(c, spot=float("inf"))
    assert read(db)["reasons"] == ["invalid_registry_spot"]


def test_postgres_uncommitted_batch_invisible_and_backdated_registration_not_proof(db):
    with db.connect() as writer:
        tx = writer.begin()
        insert(writer)
        assert read(db)["reasons"] == ["capture_not_found"]
        tx.commit()
    result = read(db)
    assert result["diagnostic_complete"]
    assert "postcommit_visibility_unproven" in result["reasons"]
    assert not result["research_eligible"]


def test_postgres_missing_batch_never_uses_other_batch(db):
    with db.begin() as c:
        insert(c, batch="B")
    assert read(db)["reasons"] == ["capture_not_found"]
