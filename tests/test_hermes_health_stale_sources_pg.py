"""DB-gated tests for scripts/hermes_health.py::check_db_health's
cadence-aware stale_sources signal (GRID-STALE-SOURCES-AUDIT-20260929.md).

Runs only against ``GRID_TEST_DB_URL`` (a disposable PostgreSQL --
``postgresql://grid:testpass@localhost:5432/griddb_test`` in CI, per
``.github/workflows/test.yml``'s ephemeral-Postgres steps), mirroring
``tests/godview/test_fed_liquidity_writer_pg.py``'s throwaway-schema
pattern. The fix's per-source freshness read uses a Postgres-only
``LATERAL`` join, so this is the only place it can be exercised for real
rather than just inspected as source text (see
``tests/test_hermes_health.py`` for the always-on inspection tests).
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text

pytestmark = pytest.mark.integration

_SCHEMA_DDL = (
    """
    CREATE TABLE source_catalog (
        id                SERIAL PRIMARY KEY,
        name              TEXT NOT NULL UNIQUE,
        base_url          TEXT NOT NULL DEFAULT 'https://example.test',
        cost_tier         TEXT NOT NULL DEFAULT 'FREE',
        latency_class     TEXT NOT NULL DEFAULT 'EOD',
        pit_available     BOOLEAN NOT NULL DEFAULT FALSE,
        revision_behavior TEXT NOT NULL DEFAULT 'NEVER',
        trust_score       TEXT NOT NULL DEFAULT 'MED',
        priority_rank     INTEGER NOT NULL DEFAULT 50,
        active            BOOLEAN NOT NULL DEFAULT TRUE,
        last_pull_at      TIMESTAMPTZ,
        uptime_score      DOUBLE PRECISION NOT NULL DEFAULT 1.0,
        created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE raw_series (
        id                BIGSERIAL PRIMARY KEY,
        series_id         TEXT NOT NULL,
        source_id         INTEGER NOT NULL REFERENCES source_catalog(id),
        obs_date          DATE NOT NULL,
        pull_timestamp    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        value             DOUBLE PRECISION NOT NULL,
        raw_payload       JSONB,
        pull_status       TEXT NOT NULL CHECK (pull_status IN ('SUCCESS', 'PARTIAL', 'FAILED', 'QUARANTINED'))
    )
    """,
    "CREATE INDEX idx_raw_series_status_source_pull ON raw_series(pull_status, source_id, pull_timestamp DESC)",
)


def _require_test_db_url() -> str:
    db_url = os.environ.get("GRID_TEST_DB_URL")
    if not db_url:
        pytest.skip("GRID_TEST_DB_URL not set")
    return db_url


@pytest.fixture
def health_engine():
    db_url = _require_test_db_url()
    root_engine = create_engine(db_url, pool_pre_ping=True)
    try:
        with root_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:  # noqa: BLE001
        root_engine.dispose()
        pytest.skip(f"GRID_TEST_DB_URL set but unreachable: {exc}")

    schema = f"hermes_health_{uuid.uuid4().hex[:12]}"
    with root_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))

    engine = create_engine(
        db_url,
        pool_size=1,
        max_overflow=0,
        connect_args={"options": f"-csearch_path={schema}"},
    )
    try:
        with engine.begin() as conn:
            for ddl in _SCHEMA_DDL:
                conn.execute(text(ddl))
        yield engine
    finally:
        engine.dispose()
        with root_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        root_engine.dispose()


def _add_source(conn, name, *, last_pull_at=None, active=True, latency_class="EOD"):
    row = conn.execute(
        text(
            "INSERT INTO source_catalog (name, last_pull_at, active, latency_class) "
            "VALUES (:name, :lp, :active, :lc) RETURNING id"
        ),
        {"name": name, "lp": last_pull_at, "active": active, "lc": latency_class},
    ).fetchone()
    return row[0]


def _add_success_pull(conn, source_id, pull_timestamp, obs_date=None):
    conn.execute(
        text(
            "INSERT INTO raw_series (series_id, source_id, obs_date, pull_timestamp, value, pull_status) "
            "VALUES ('X', :sid, :obs, :ts, 1.0, 'SUCCESS')"
        ),
        {"sid": source_id, "obs": obs_date or pull_timestamp.date(), "ts": pull_timestamp},
    )


def _stale_names(result) -> set[str]:
    return {s["source"] for s in result["stale_sources"]}


class TestCadenceAwareStaleSources:
    def test_weekly_source_within_cadence_is_not_flagged(self, health_engine):
        """EIA is in the audit-derived override map as WEEKLY. 5 days old
        would have failed the old flat 26h rule but is well within an
        8-day weekly grace."""
        from scripts.hermes_health import check_db_health

        now = datetime.now(timezone.utc)
        with health_engine.begin() as conn:
            _add_source(conn, "EIA", last_pull_at=now - timedelta(days=5))

        result = check_db_health(health_engine)

        assert result["healthy"] is True
        assert "EIA" not in _stale_names(result)

    def test_writer_that_never_bumps_catalog_is_read_from_raw_series(self, health_engine):
        """treasury_auction is in the override map as DAILY. Its
        source_catalog.last_pull_at is NULL (the audit's "writer never
        bumps" pattern), but raw_series has a fresh SUCCESS row -- the fix
        must use the newer of the two, not just the catalog column."""
        from scripts.hermes_health import check_db_health

        now = datetime.now(timezone.utc)
        with health_engine.begin() as conn:
            source_id = _add_source(conn, "treasury_auction", last_pull_at=None)
            _add_success_pull(conn, source_id, now - timedelta(hours=2))

        result = check_db_health(health_engine)

        assert "treasury_auction" not in _stale_names(result)

    def test_genuinely_broken_daily_source_is_still_flagged(self, health_engine):
        """A source with no override, no catalog metadata, no recent
        raw_series row, and a 3-day-old catalog timestamp must still trip
        the alert -- the fix must not become a blanket exemption."""
        from scripts.hermes_health import check_db_health

        now = datetime.now(timezone.utc)
        with health_engine.begin() as conn:
            _add_source(conn, "totally_broken_source", last_pull_at=now - timedelta(days=3))

        result = check_db_health(health_engine)

        stale_by_name = {s["source"]: s for s in result["stale_sources"]}
        assert "totally_broken_source" in stale_by_name
        entry = stale_by_name["totally_broken_source"]
        assert entry["cadence"] == "DAILY"
        assert entry["age_hours"] is not None and entry["age_hours"] > 30

    def test_never_pulled_source_is_flagged_as_never(self, health_engine):
        from scripts.hermes_health import check_db_health

        with health_engine.begin() as conn:
            _add_source(conn, "brand_new_unpulled_source", last_pull_at=None)

        result = check_db_health(health_engine)

        stale_by_name = {s["source"]: s for s in result["stale_sources"]}
        assert stale_by_name["brand_new_unpulled_source"]["last_pull"] == "never"

    def test_inactive_source_is_never_considered(self, health_engine):
        from scripts.hermes_health import check_db_health

        now = datetime.now(timezone.utc)
        with health_engine.begin() as conn:
            _add_source(conn, "retired_inactive_source", last_pull_at=now - timedelta(days=400), active=False)

        result = check_db_health(health_engine)

        assert "retired_inactive_source" not in _stale_names(result)

    def test_update_frequency_column_is_read_opportunistically_when_present(self, health_engine):
        """update_frequency isn't in schema.sql, but the audit found it on
        the live catalog. A source not in the override map should still
        pick up a recognized value from it as a fallback cadence."""
        from scripts.hermes_health import check_db_health

        now = datetime.now(timezone.utc)
        with health_engine.begin() as conn:
            conn.execute(text("ALTER TABLE source_catalog ADD COLUMN update_frequency TEXT"))
            source_id = _add_source(conn, "some_new_weekly_feed", last_pull_at=now - timedelta(days=5))
            conn.execute(
                text("UPDATE source_catalog SET update_frequency = 'WEEKLY' WHERE id = :id"),
                {"id": source_id},
            )

        result = check_db_health(health_engine)

        assert "some_new_weekly_feed" not in _stale_names(result)

    def test_missing_update_frequency_column_does_not_error(self, health_engine):
        """Baseline schema (no update_frequency column at all, as in
        schema.sql) must not raise -- the information_schema check must
        gate the read cleanly."""
        from scripts.hermes_health import check_db_health

        now = datetime.now(timezone.utc)
        with health_engine.begin() as conn:
            _add_source(conn, "plain_schema_source", last_pull_at=now - timedelta(hours=1))

        result = check_db_health(health_engine)

        assert result["healthy"] is True
        assert "error" not in result
