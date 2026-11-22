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


# Set by the dedicated CI step (.github/workflows/test.yml, "Hermes
# stale-sources PostgreSQL contract") so a missing or unreachable database
# fails that step instead of skipping every test in it.
_REQUIRE_ENV = "REQUIRE_HERMES_STALE_SOURCES_PG"


def _skip_or_fail(reason: str) -> None:
    if os.environ.get(_REQUIRE_ENV) == "1":
        pytest.fail(f"{reason} ({_REQUIRE_ENV}=1)")
    pytest.skip(reason)


def _require_test_db_url() -> str:
    db_url = os.environ.get("GRID_TEST_DB_URL")
    if not db_url:
        _skip_or_fail("GRID_TEST_DB_URL not set")
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
        _skip_or_fail(f"GRID_TEST_DB_URL set but unreachable: {exc}")

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

    def test_genuinely_broken_daily_source_is_still_flagged(self, health_engine, monkeypatch):
        """A source with no override, no catalog metadata, no recent
        raw_series row, and a 3-day-old catalog timestamp must still trip
        the alert -- the fix must not become a blanket exemption."""
        from scripts.hermes_health import check_db_health

        # Pin a weekday: a relative Friday->Monday fixture correctly remains
        # within DAILY's weekend grace and cannot prove a broken daily source.
        now = datetime(2026, 9, 23, 12, tzinfo=timezone.utc)

        class Clock(datetime):
            @classmethod
            def now(cls, tz=None):
                return now.astimezone(tz) if tz else now.replace(tzinfo=None)

        monkeypatch.setattr("scripts.hermes_health.datetime", Clock)

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


class TestStaleSourceLastAttempt:
    """Each stale entry carries its latest pull_log attempt, so the alert
    can say why a source is stale instead of only that it is."""

    _PULL_LOG_DDL = (
        "CREATE TABLE pull_log ("
        "  id BIGSERIAL PRIMARY KEY, puller_name TEXT, source_id INTEGER,"
        "  started_at TIMESTAMPTZ NOT NULL, completed_at TIMESTAMPTZ,"
        "  status TEXT NOT NULL, rows_inserted INTEGER, error_message TEXT,"
        "  node_name TEXT)"
    )

    def _log(self, conn, source_id, started_at, status, error):
        conn.execute(
            text(
                "INSERT INTO pull_log (puller_name, source_id, started_at, completed_at, status, error_message) "
                "VALUES ('smart:x', :sid, :st, :st, :status, :err)"
            ),
            {"sid": source_id, "st": started_at, "status": status, "err": error},
        )

    def test_latest_attempt_reason_and_not_configured_flag(self, health_engine):
        from scripts.hermes_health import check_db_health

        now = datetime.now(timezone.utc)
        with health_engine.begin() as conn:
            conn.execute(text(self._PULL_LOG_DDL))
            broken = _add_source(conn, "broken_contract_source", last_pull_at=None)
            off = _add_source(conn, "unconfigured_source", last_pull_at=None)
            _add_source(conn, "never_attempted_source", last_pull_at=None)
            self._log(conn, broken, now - timedelta(hours=5), "SUCCESS", None)
            self._log(conn, broken, now - timedelta(hours=1), "FAILED",
                      "PullerReportedFailure: puller did not report committed write count")
            self._log(conn, off, now - timedelta(hours=2), "FAILED", "PullerReportedFailure: no_api_key")

        result = check_db_health(health_engine)

        by_name = {s["source"]: s for s in result["stale_sources"]}
        b = by_name["broken_contract_source"]
        assert b["last_attempt_status"] == "FAILED"
        assert "committed write count" in b["last_attempt_reason"]
        assert b["not_configured"] is False
        assert by_name["unconfigured_source"]["not_configured"] is True
        n = by_name["never_attempted_source"]
        assert n["last_attempt_status"] is None and n["not_configured"] is False
        # The health transaction survived the lookup: later keys still populated.
        assert result["healthy"] is True


class TestSharedCatalogRowJudgedByOwnSeries:
    """FINRA_SHORT_VOLUME is judged by trade-date coverage of the short-volume series.

    finra_dix writes finra:dix_sp500 under the same catalog row and the
    SmartScheduler bumps its last_pull_at for every DIX run, so on
    2026-11-18..11-20 (no short-volume obs; DIX landing each morning) the
    old "newer of last_pull_at and the newest row for the source" rule read
    it fresh and neither the alert nor Hermes's stale-refresh saw the gap.
    The trade date due at 11-20 14:00Z is 11-18 (pinned XNYS calendar,
    ~18:00 ET release, one 24 h cadence). See
    ingestion/source_freshness.py::SERIES_ANCHORS.
    """

    # Friday 2026-11-20 14:00Z, the third skipped trade date in a row.
    NOW = datetime(2026, 11, 20, 14, tzinfo=timezone.utc)
    SHORT_VOLUME_LANDED = datetime(2026, 11, 18, 7, 50, tzinfo=timezone.utc)  # obs 11-17
    DIX_LANDED = datetime(2026, 11, 20, 7, 30, tzinfo=timezone.utc)           # obs 11-19

    @pytest.fixture(autouse=True)
    def _clock(self, monkeypatch):
        now = self.NOW

        class Clock(datetime):
            @classmethod
            def now(cls, tz=None):
                return now.astimezone(tz) if tz else now.replace(tzinfo=None)

        monkeypatch.setattr("scripts.hermes_health.datetime", Clock)

    @staticmethod
    def _row(conn, source_id, series_id, obs_date, landed):
        conn.execute(
            text(
                "INSERT INTO raw_series (series_id, source_id, obs_date, pull_timestamp, value, pull_status) "
                "VALUES (:series, :sid, :obs, :ts, 0.48, 'SUCCESS')"
            ),
            {"series": series_id, "sid": source_id, "obs": obs_date, "ts": landed},
        )

    def _dix_only_day(self, conn):
        finra = _add_source(conn, "FINRA_SHORT_VOLUME", last_pull_at=self.DIX_LANDED)
        for series in ("finra:short_volume_agg:market_ratio", "finra:short_volume:SPY"):
            self._row(conn, finra, series, self.SHORT_VOLUME_LANDED.date() - timedelta(days=1),
                      self.SHORT_VOLUME_LANDED)
        self._row(conn, finra, "finra:dix_sp500", self.DIX_LANDED.date() - timedelta(days=1),
                  self.DIX_LANDED)
        return finra

    def test_dix_only_day_reads_stale_for_the_short_volume_puller(self, health_engine):
        from scripts.hermes_health import check_db_health

        with health_engine.begin() as conn:
            self._dix_only_day(conn)

        result = check_db_health(health_engine)

        assert "error" not in result
        by_name = {s["source"]: s for s in result["stale_sources"]}
        entry = by_name["FINRA_SHORT_VOLUME"]
        assert entry["last_pull"] == self.SHORT_VOLUME_LANDED.isoformat()  # landing: context only
        assert entry["cadence"] == "TRADE_DATE"
        assert entry["age_hours"] == pytest.approx(54.2, abs=0.1)
        judged = entry["judged_by"]
        assert judged["owner"] == "finra_short_volume"
        assert judged["covered"] is False
        assert judged["expected_obs_date"] == "2026-11-18"
        assert judged["newest_obs_date"] == "2026-11-17"
        assert {s["series_id"]: s["obs_date"] for s in judged["series"]} == {
            "finra:short_volume_agg:market_ratio": "2026-11-17",
            "finra:short_volume:SPY": "2026-11-17",
        }

    def test_fresh_short_volume_series_is_not_flagged(self, health_engine):
        from scripts.hermes_health import check_db_health

        landed = datetime(2026, 11, 20, 8, 5, tzinfo=timezone.utc)
        with health_engine.begin() as conn:
            finra = self._dix_only_day(conn)
            for series in ("finra:short_volume_agg:market_ratio", "finra:short_volume:SPY"):
                self._row(conn, finra, series, landed.date() - timedelta(days=1), landed)

        result = check_db_health(health_engine)

        assert "FINRA_SHORT_VOLUME" not in _stale_names(result)

    def test_old_trade_date_landed_again_is_flagged(self, health_engine):
        """A repair or re-pull of obs 11-10 that lands at 13:40Z on 11-20 is not 11-18."""
        from scripts.hermes_health import check_db_health

        relanded = datetime(2026, 11, 20, 13, 40, tzinfo=timezone.utc)
        with health_engine.begin() as conn:
            finra = _add_source(conn, "FINRA_SHORT_VOLUME", last_pull_at=relanded)
            for series in ("finra:short_volume_agg:market_ratio", "finra:short_volume:SPY"):
                self._row(conn, finra, series, datetime(2026, 11, 10).date(), relanded)

        result = check_db_health(health_engine)

        entry = {s["source"]: s for s in result["stale_sources"]}["FINRA_SHORT_VOLUME"]
        assert entry["last_pull"] == relanded.isoformat()
        assert entry["age_hours"] == pytest.approx(0.3, abs=0.1)
        assert entry["judged_by"]["newest_obs_date"] == "2026-11-10"
        assert entry["judged_by"]["expected_obs_date"] == "2026-11-18"

    def test_dix_attempt_is_not_the_short_volume_last_attempt(self, health_engine):
        """A DIX "no TIINGO_API_KEY" skip must not mark short volume not_configured."""
        from scripts.hermes_health import check_db_health

        with health_engine.begin() as conn:
            conn.execute(text(TestStaleSourceLastAttempt._PULL_LOG_DDL))
            finra = self._dix_only_day(conn)
            for name, started, status, error in (
                ("smart:finra_short_volume", datetime(2026, 11, 20, 9, tzinfo=timezone.utc),
                 "SUCCESS", "NO_NEW_DATA: run completed, 0 rows written"),
                ("smart:finra_dix", datetime(2026, 11, 20, 10, tzinfo=timezone.utc),
                 "SUCCESS", "SKIPPED: missing API key TIINGO_API_KEY not configured"),
            ):
                conn.execute(
                    text(
                        "INSERT INTO pull_log (puller_name, source_id, started_at, completed_at, "
                        "status, error_message) VALUES (:n, :sid, :st, :st, :status, :err)"
                    ),
                    {"n": name, "sid": finra, "st": started, "status": status, "err": error},
                )

        result = check_db_health(health_engine)

        entry = {s["source"]: s for s in result["stale_sources"]}["FINRA_SHORT_VOLUME"]
        assert entry["last_attempt_reason"].startswith("NO_NEW_DATA")
        assert entry["not_configured"] is False
        assert result["healthy"] is True
