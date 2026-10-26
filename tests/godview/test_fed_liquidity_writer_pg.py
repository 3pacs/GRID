"""DB-gated tests for godview/fed_liquidity.py.

Runs only against ``GRID_TEST_DB_URL`` (a disposable PostgreSQL --
``postgresql://grid:testpass@localhost:5432/griddb_test`` in CI, per
``.github/workflows/test.yml``'s ephemeral-Postgres steps). Every test gets
its own throwaway schema, created fresh and dropped in a fixture ``finally``,
built by running #674's real ``god_view_market_tables_20260918`` migration
followed by the real ``godview_writers_20260926`` migration (G2) -- so the
tables, columns and CHECK constraints this writer depends on are the actual
production ones, not a hand-copied approximation. This mirrors
``tests/test_godview_schema_migration_pg.py``'s own fixture pattern.

Each test class here corresponds to one blocker from the independent review
of PR #677 @ 79faa3c4:

* ``TestB1RrpUnits`` -- production stores reverse_repo_rrp in millions, not
  raw billions; this writer must too.
* ``TestB2LegacyContamination`` -- delta/peak history must never read a
  legacy (provenance IS NULL) row.
* ``TestB3NeverTouchLegacyRow`` -- the writer must refuse to update or
  displace a legacy row, and report the A2-archive prerequisite instead.

Plus the general arithmetic / missing-input / PIT / idempotency coverage
from the original PR.
"""

from __future__ import annotations

import importlib
import os
import uuid
from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from godview.fed_liquidity import (
    RRP_SERIES_ID,
    SKIP_BEFORE_RELEASE,
    SKIP_BLOCKED_BY_LEGACY_ROW,
    SKIP_MISSING_RRP,
    SKIP_MISSING_WALCL,
    STATUS_PARTIAL_BLOCKED_BY_LEGACY,
    WALCL_SERIES_ID,
    WTREGEN_SERIES_ID,
    compute_release_at,
    materialize_fed_liquidity,
)

pytestmark = pytest.mark.integration

_CODE_SHA = "test-sha-godview-g3"

_BASE_MIGRATION = "migrations.versions.god_view_market_tables_20260918"
_G2_MIGRATION = "migrations.versions.godview_writers_20260926"

_PREREQ_DDL = (
    """
    CREATE TABLE market_briefings (
        id              SERIAL PRIMARY KEY,
        briefing_type   TEXT NOT NULL,
        briefing_date   DATE NOT NULL,
        content         TEXT NOT NULL,
        snapshot_data   JSONB,
        created_at      TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE insider_trades (
        id          SERIAL PRIMARY KEY,
        trade_date  DATE,
        trade_type  TEXT
    )
    """,
    # store.observations.read_window joins source_catalog and raw_series --
    # not part of either godview migration, so this fixture provides the
    # same minimal shape tests/conftest.py-style fixtures use elsewhere.
    """
    CREATE TABLE source_catalog (
        id                SERIAL PRIMARY KEY,
        name              TEXT NOT NULL UNIQUE,
        base_url          TEXT NOT NULL DEFAULT 'https://fred.stlouisfed.org',
        cost_tier         TEXT NOT NULL DEFAULT 'FREE',
        latency_class     TEXT NOT NULL DEFAULT 'WEEKLY',
        pit_available     BOOLEAN NOT NULL DEFAULT TRUE,
        revision_behavior TEXT NOT NULL DEFAULT 'RARE',
        trust_score       TEXT NOT NULL DEFAULT 'HIGH',
        priority_rank     INTEGER NOT NULL DEFAULT 10
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
        pull_status       TEXT NOT NULL CHECK (pull_status IN ('SUCCESS', 'PARTIAL', 'FAILED'))
    )
    """,
    "INSERT INTO source_catalog (name) VALUES ('FRED')",
)


def _run_migration(engine: Engine, module: str, fn_name: str) -> None:
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    migration = importlib.import_module(module)
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            real_op = migration.op
            migration.op = Operations(MigrationContext.configure(conn))
            try:
                getattr(migration, fn_name)()
            finally:
                migration.op = real_op
            trans.commit()
        except Exception:
            trans.rollback()
            raise


def _require_test_db_url() -> str:
    db_url = os.environ.get("GRID_TEST_DB_URL")
    if not db_url:
        pytest.skip("GRID_TEST_DB_URL not set")
    return db_url


@pytest.fixture
def godview_engine():
    db_url = _require_test_db_url()
    root_engine = create_engine(db_url, pool_pre_ping=True)
    try:
        with root_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:  # noqa: BLE001
        root_engine.dispose()
        pytest.skip(f"GRID_TEST_DB_URL set but unreachable: {exc}")

    schema = f"godview_fed_{uuid.uuid4().hex[:12]}"
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
            for ddl in _PREREQ_DDL:
                conn.execute(text(ddl))
        _run_migration(engine, _BASE_MIGRATION, "upgrade")
        _run_migration(engine, _G2_MIGRATION, "upgrade")
        yield engine
    finally:
        engine.dispose()
        with root_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        root_engine.dispose()


def _fred_source_id(conn) -> int:
    return conn.execute(text("SELECT id FROM source_catalog WHERE name = 'FRED'")).scalar()


def _insert_component(conn, series_id: str, obs_date: date, value: float, *, pull_timestamp: datetime) -> None:
    conn.execute(
        text(
            "INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_timestamp, pull_status) "
            "VALUES (:sid, :src, :od, :val, :pts, 'SUCCESS')"
        ),
        {"sid": series_id, "src": _fred_source_id(conn), "od": obs_date, "val": value, "pts": pull_timestamp},
    )


def _insert_legacy_row(
    conn,
    obs_date: date,
    *,
    walcl: float,
    wtregen: float,
    rrp_stored: float,
    net_liquidity_usd_m: float,
    liquidity_regime: str | None = "STABLE",
) -> None:
    """A row with provenance left NULL, exactly as the untracked incident
    materializer -- or the pre-G3 historical rows -- left them. rrp_stored
    is whatever raw value the legacy writer happened to put in the column
    (this fixture is deliberately free to store it either way, since B1's
    whole point is that THIS writer must not assume the legacy convention
    when reading its OWN history back -- B2 solves that by excluding
    legacy rows from history entirely)."""
    conn.execute(
        text(
            "INSERT INTO fed_net_liquidity_daily "
            "(obs_date, fed_assets_walcl, treasury_tga_wtregen, reverse_repo_rrp, "
            " net_liquidity_usd_m, liquidity_regime) "
            "VALUES (:d, :w, :t, :r, :n, :lr)"
        ),
        {"d": obs_date, "w": walcl, "t": wtregen, "r": rrp_stored, "n": net_liquidity_usd_m, "lr": liquidity_regime},
    )


def _row(conn, obs_date: date):
    return conn.execute(
        text(
            "SELECT fed_assets_walcl, treasury_tga_wtregen, reverse_repo_rrp, net_liquidity_usd_m, "
            "rrp_as_pct_of_peak, delta_1w_m, delta_4w_m, liquidity_regime, "
            "provenance, availability_basis, source_ref, run_id, code_sha, updated_at, coverage_fraction "
            "FROM fed_net_liquidity_daily WHERE obs_date = :d"
        ),
        {"d": obs_date},
    ).mappings().fetchone()


def _run_ledger_row(conn, run_id: str):
    return conn.execute(
        text("SELECT * FROM godview_runs WHERE run_id = CAST(:r AS UUID)"), {"r": run_id}
    ).mappings().fetchone()


# ══════════════════════════════════════════════════════════════════════════
# Correct arithmetic / units
# ══════════════════════════════════════════════════════════════════════════


def test_writes_a_row_with_correctly_scaled_net_liquidity_after_release(godview_engine):
    engine = godview_engine
    obs_date = date(2026, 9, 16)  # a Wednesday
    release_at, _ = compute_release_at(obs_date)
    after_release = release_at + timedelta(hours=4)  # e.g. pulled Thursday ~20:30 ET

    with engine.begin() as conn:
        _insert_component(conn, WALCL_SERIES_ID, obs_date, 7_500_000.0, pull_timestamp=after_release)
        _insert_component(conn, WTREGEN_SERIES_ID, obs_date, 700_000.0, pull_timestamp=after_release)
        _insert_component(conn, RRP_SERIES_ID, obs_date, 300.0, pull_timestamp=after_release)

    result = materialize_fed_liquidity(engine, code_sha=_CODE_SHA, as_of_ts=after_release + timedelta(minutes=5))

    assert result.status == "SUCCESS"
    assert result.rows_written == 1
    assert result.code_sha == _CODE_SHA

    with engine.begin() as conn:
        row = _row(conn, obs_date)
        ledger = _run_ledger_row(conn, result.run_id)
    assert row is not None
    # 7,500,000M WALCL - 700,000M WTREGEN - 300B RRP (=300,000M) = 6,500,000M
    assert row["net_liquidity_usd_m"] == pytest.approx(6_500_000.0)
    assert row["liquidity_regime"] == "insufficient_history"  # no 4w-prior row exists yet
    assert row["provenance"] == "measured"
    assert row["availability_basis"] == "observed_acquisition"
    assert row["code_sha"] == _CODE_SHA
    assert row["source_ref"] is not None
    assert row["run_id"] is not None
    assert ledger is not None
    assert ledger["status"] == "complete"
    assert ledger["rows_written"] == 1


class TestB1RrpUnits:
    """B1: reverse_repo_rrp must be stored in millions, matching production's
    own convention (verified on the real 09-16 row:
    6,740,619 - 877,028 - 5,375 = 5,858,216 -- 5,375 IS 5.375bn in millions)."""

    def test_reverse_repo_rrp_is_stored_in_millions_not_raw_billions(self, godview_engine):
        engine = godview_engine
        obs_date = date(2026, 9, 16)
        release_at, _ = compute_release_at(obs_date)
        after_release = release_at + timedelta(hours=4)

        with engine.begin() as conn:
            _insert_component(conn, WALCL_SERIES_ID, obs_date, 6_740_619.0, pull_timestamp=after_release)
            _insert_component(conn, WTREGEN_SERIES_ID, obs_date, 877_028.0, pull_timestamp=after_release)
            _insert_component(conn, RRP_SERIES_ID, obs_date, 5.375, pull_timestamp=after_release)  # raw FRED billions

        materialize_fed_liquidity(engine, code_sha=_CODE_SHA, as_of_ts=after_release + timedelta(minutes=5))

        with engine.begin() as conn:
            row = _row(conn, obs_date)
        # Stored value must be 5,375 (millions), matching production's real
        # 09-16 row -- never 5.375 (the raw billions value read from raw_series).
        assert row["reverse_repo_rrp"] == pytest.approx(5375.0)
        assert row["net_liquidity_usd_m"] == pytest.approx(6_740_619.0 - 877_028.0 - 5375.0)
        # The stored legs combine directly with no separate scale factor.
        assert row["fed_assets_walcl"] - row["treasury_tga_wtregen"] - row["reverse_repo_rrp"] == pytest.approx(
            row["net_liquidity_usd_m"]
        )

    def test_rrp_as_pct_of_peak_is_not_inflated_1000x_by_unit_mismatch(self, godview_engine):
        """With B1 fixed, a rising RRP across several of this writer's own
        weeks should read back a sane, non-near-zero pct-of-peak, not the
        ~0% the review flagged when history was misread as 1000x too large."""
        engine = godview_engine
        base = date(2026, 3, 4)  # a Wednesday
        rrp_billions_by_week = [100.0, 150.0, 200.0, 250.0]  # rising RRP
        for i, rrp_b in enumerate(rrp_billions_by_week):
            obs_date = base + timedelta(weeks=i)
            release_at, _ = compute_release_at(obs_date)
            after_release = release_at + timedelta(hours=4)
            with engine.begin() as conn:
                _insert_component(conn, WALCL_SERIES_ID, obs_date, 7_000_000.0, pull_timestamp=after_release)
                _insert_component(conn, WTREGEN_SERIES_ID, obs_date, 700_000.0, pull_timestamp=after_release)
                _insert_component(conn, RRP_SERIES_ID, obs_date, rrp_b, pull_timestamp=after_release)
            materialize_fed_liquidity(engine, code_sha=_CODE_SHA, as_of_ts=after_release + timedelta(minutes=5))

        last_obs_date = base + timedelta(weeks=len(rrp_billions_by_week) - 1)
        with engine.begin() as conn:
            row = _row(conn, last_obs_date)
        # The last week is the peak (rising series) -> ~100%, not ~0.1% (the
        # 1000x-deflated value a stale-unit bug would produce reading its own
        # correctly-stored millions back as if they were still billions).
        assert row["rrp_as_pct_of_peak"] == pytest.approx(100.0, abs=0.5)


class TestB2LegacyContamination:
    """B2: delta/peak history must read only this writer's own
    (provenance IS NOT NULL) Wednesday rows -- never a legacy forward-filled
    or fabricated row such as the incident materializer's non-Wednesday,
    hard-coded WALCL=6,780,000 Friday row."""

    def test_delta_4w_ignores_a_legacy_row_at_the_target_gap(self, godview_engine):
        engine = godview_engine
        legacy_friday = date(2026, 8, 21)  # NOT a Wednesday -- like the incident's forward-filled rows
        obs_date = date(2026, 9, 16)  # a Wednesday, ~4 weeks (26 days) after the legacy Friday

        with engine.begin() as conn:
            # Legacy row sitting almost exactly at the delta_4w target gap,
            # with a wildly fabricated WALCL (mirrors finding 3).
            _insert_legacy_row(
                conn, legacy_friday, walcl=6_780_000.0, wtregen=790_000.0, rrp_stored=500.0,
                net_liquidity_usd_m=99_999_999.0,  # implausible, to make contamination obvious if it leaks in
            )

        release_at, _ = compute_release_at(obs_date)
        after_release = release_at + timedelta(hours=4)
        with engine.begin() as conn:
            _insert_component(conn, WALCL_SERIES_ID, obs_date, 7_500_000.0, pull_timestamp=after_release)
            _insert_component(conn, WTREGEN_SERIES_ID, obs_date, 700_000.0, pull_timestamp=after_release)
            _insert_component(conn, RRP_SERIES_ID, obs_date, 300.0, pull_timestamp=after_release)

        materialize_fed_liquidity(engine, code_sha=_CODE_SHA, as_of_ts=after_release + timedelta(minutes=5))

        with engine.begin() as conn:
            row = _row(conn, obs_date)
        # No qualifying (provenance-marked, Wednesday) prior row exists, so
        # delta_4w_m must be NULL -- not a delta against the fabricated
        # legacy row's 99,999,999.
        assert row["delta_4w_m"] is None
        assert row["liquidity_regime"] == "insufficient_history"

    def test_rrp_pct_of_peak_ignores_a_legacy_rrp_value(self, godview_engine):
        engine = godview_engine
        legacy_date = date(2026, 8, 19)  # a Wednesday, but a legacy (unprovenanced) row
        obs_date = date(2026, 9, 16)

        with engine.begin() as conn:
            # A huge legacy RRP that would dominate the peak window if it
            # ever leaked into this writer's own history read.
            _insert_legacy_row(
                conn, legacy_date, walcl=7_000_000.0, wtregen=700_000.0, rrp_stored=999_999_999.0,
                net_liquidity_usd_m=1.0,
            )

        release_at, _ = compute_release_at(obs_date)
        after_release = release_at + timedelta(hours=4)
        with engine.begin() as conn:
            _insert_component(conn, WALCL_SERIES_ID, obs_date, 7_000_000.0, pull_timestamp=after_release)
            _insert_component(conn, WTREGEN_SERIES_ID, obs_date, 700_000.0, pull_timestamp=after_release)
            _insert_component(conn, RRP_SERIES_ID, obs_date, 300.0, pull_timestamp=after_release)

        materialize_fed_liquidity(engine, code_sha=_CODE_SHA, as_of_ts=after_release + timedelta(minutes=5))

        with engine.begin() as conn:
            row = _row(conn, obs_date)
        # Below _MIN_HISTORY_FOR_PEAK (4) once the legacy row is correctly
        # excluded, so pct_of_peak is NULL -- not a near-zero percentage
        # against the legacy row's 999,999,999.
        assert row["rrp_as_pct_of_peak"] is None


class TestB3NeverTouchLegacyRow:
    """B3: obs_date is still the sole unique key -- a legacy row and a new
    provenance-marked row can never coexist for the same date. This writer
    must refuse to update or displace a legacy row, and report the gap
    instead of overwriting evidence."""

    def test_a_legacy_row_is_never_updated_and_the_date_is_skipped(self, godview_engine):
        engine = godview_engine
        obs_date = date(2026, 4, 8)  # one of the plan's 7 real fabricated dates (04-02..05-19)
        release_at, _ = compute_release_at(obs_date)
        after_release = release_at + timedelta(hours=4)

        with engine.begin() as conn:
            _insert_legacy_row(
                conn, obs_date, walcl=6_780_000.0, wtregen=790_000.0, rrp_stored=500.0,
                net_liquidity_usd_m=5_490_000.0, liquidity_regime="STABLE",
            )
            _insert_component(conn, WALCL_SERIES_ID, obs_date, 7_400_000.0, pull_timestamp=after_release)
            _insert_component(conn, WTREGEN_SERIES_ID, obs_date, 650_000.0, pull_timestamp=after_release)
            _insert_component(conn, RRP_SERIES_ID, obs_date, 280.0, pull_timestamp=after_release)

        result = materialize_fed_liquidity(engine, code_sha=_CODE_SHA, as_of_ts=after_release + timedelta(minutes=5))

        assert result.rows_written == 0
        assert any(r.obs_date == obs_date and r.reason == SKIP_BLOCKED_BY_LEGACY_ROW for r in result.rows)
        # A run that was blocked by a legacy row must never report as plain
        # success -- a caller/health-check reading only .status needs to
        # see that this date is stuck pending the plan's A2 archive.
        assert result.status == STATUS_PARTIAL_BLOCKED_BY_LEGACY

        with engine.begin() as conn:
            row = _row(conn, obs_date)
        # The legacy row is untouched, byte-for-byte: still the fabricated
        # WALCL/TGA, still provenance NULL, never overwritten by this run.
        assert row["fed_assets_walcl"] == pytest.approx(6_780_000.0)
        assert row["treasury_tga_wtregen"] == pytest.approx(790_000.0)
        assert row["provenance"] is None
        assert row["liquidity_regime"] == "STABLE"

    def test_a_second_writer_row_for_a_different_date_still_writes_normally(self, godview_engine):
        """The legacy-row block is per-obs_date, not a whole-run abort."""
        engine = godview_engine
        legacy_date = date(2026, 4, 8)
        clean_date = date(2026, 9, 16)

        legacy_release_at, _ = compute_release_at(legacy_date)
        legacy_after_release = legacy_release_at + timedelta(hours=4)
        with engine.begin() as conn:
            _insert_legacy_row(
                conn, legacy_date, walcl=6_780_000.0, wtregen=790_000.0, rrp_stored=500.0,
                net_liquidity_usd_m=5_490_000.0,
            )
            # A candidate must have FRED observations to be considered at
            # all -- otherwise it never reaches the legacy-row pre-check,
            # and this test would (wrongly) pass by never exercising it.
            _insert_component(conn, WALCL_SERIES_ID, legacy_date, 7_400_000.0, pull_timestamp=legacy_after_release)
            _insert_component(conn, WTREGEN_SERIES_ID, legacy_date, 650_000.0, pull_timestamp=legacy_after_release)
            _insert_component(conn, RRP_SERIES_ID, legacy_date, 280.0, pull_timestamp=legacy_after_release)

        clean_release_at, _ = compute_release_at(clean_date)
        clean_after_release = clean_release_at + timedelta(hours=4)
        with engine.begin() as conn:
            _insert_component(conn, WALCL_SERIES_ID, clean_date, 7_500_000.0, pull_timestamp=clean_after_release)
            _insert_component(conn, WTREGEN_SERIES_ID, clean_date, 700_000.0, pull_timestamp=clean_after_release)
            _insert_component(conn, RRP_SERIES_ID, clean_date, 300.0, pull_timestamp=clean_after_release)

        as_of_ts = max(legacy_after_release, clean_after_release) + timedelta(minutes=5)
        result = materialize_fed_liquidity(engine, code_sha=_CODE_SHA, as_of_ts=as_of_ts)

        assert result.rows_written == 1
        assert any(r.obs_date == clean_date and r.status == "written" for r in result.rows)
        assert any(r.obs_date == legacy_date and r.reason == SKIP_BLOCKED_BY_LEGACY_ROW for r in result.rows)
        # Even though clean_date DID write, the run still carries the
        # legacy-block signal -- PARTIAL_BLOCKED_BY_LEGACY, not plain SUCCESS.
        assert result.status == STATUS_PARTIAL_BLOCKED_BY_LEGACY

    def test_our_own_prior_row_can_still_be_updated(self, godview_engine):
        """The B3 protection is specific to provenance IS NULL -- a row this
        writer wrote itself must remain updatable (e.g. a revised input)."""
        engine = godview_engine
        obs_date = date(2026, 6, 3)
        release_at, _ = compute_release_at(obs_date)
        after_release = release_at + timedelta(hours=4)

        with engine.begin() as conn:
            _insert_component(conn, WALCL_SERIES_ID, obs_date, 7_100_000.0, pull_timestamp=after_release)
            _insert_component(conn, WTREGEN_SERIES_ID, obs_date, 620_000.0, pull_timestamp=after_release)
            _insert_component(conn, RRP_SERIES_ID, obs_date, 250.0, pull_timestamp=after_release)

        as_of_ts = after_release + timedelta(minutes=5)
        first = materialize_fed_liquidity(engine, code_sha=_CODE_SHA, as_of_ts=as_of_ts)
        assert first.rows_written == 1

        # A revised WALCL vintage for the same obs_date.
        revised_pull = after_release + timedelta(hours=2)
        with engine.begin() as conn:
            _insert_component(conn, WALCL_SERIES_ID, obs_date, 7_150_000.0, pull_timestamp=revised_pull)

        second = materialize_fed_liquidity(engine, code_sha=_CODE_SHA, as_of_ts=revised_pull + timedelta(minutes=5))
        assert second.rows_written == 1  # updated, not blocked

        with engine.begin() as conn:
            row = _row(conn, obs_date)
        assert row["fed_assets_walcl"] == pytest.approx(7_150_000.0)
        assert row["provenance"] == "measured"


class TestAvailableAt:
    """available_at must be the LAST leg's pull_timestamp, never the first --
    the row is only actually knowable once every leg is in hand -- and never
    earlier than release_at itself (belt-and-suspenders: a leak that pulled
    every leg before the official release must not be reported as available
    before that release). Both fixed after an independent review of PR #677
    @ bff082af found `available_at = min(pull_timestamps)`."""

    def test_available_at_is_the_last_leg_not_the_first(self, godview_engine):
        """The exact production scenario from the review: obs_date=2026-09-23,
        RRP pulled Wed afternoon (same-day, normal cadence), but WALCL/WTREGEN
        not pulled until Thursday evening, ~32 min after the H.4.1 release.
        MIN would report available_at ~27 hours before the row was complete;
        MAX correctly reports it at the last (WALCL/WTREGEN) pull."""
        engine = godview_engine
        obs_date = date(2026, 9, 23)
        assert obs_date.weekday() == 2  # a Wednesday

        release_at, _ = compute_release_at(obs_date)
        assert release_at == datetime(2026, 9, 24, 16, 30, tzinfo=release_at.tzinfo)

        rrp_pulled = datetime(2026, 9, 23, 18, 8, tzinfo=timezone.utc)  # Wed afternoon
        walcl_wtregen_pulled = datetime(2026, 9, 24, 21, 2, tzinfo=timezone.utc)  # ~32 min after release
        assert walcl_wtregen_pulled > release_at  # sanity: after release, not a leak

        with engine.begin() as conn:
            _insert_component(conn, RRP_SERIES_ID, obs_date, 300.0, pull_timestamp=rrp_pulled)
            _insert_component(conn, WALCL_SERIES_ID, obs_date, 7_500_000.0, pull_timestamp=walcl_wtregen_pulled)
            _insert_component(conn, WTREGEN_SERIES_ID, obs_date, 700_000.0, pull_timestamp=walcl_wtregen_pulled)

        result = materialize_fed_liquidity(
            engine, code_sha=_CODE_SHA, as_of_ts=walcl_wtregen_pulled + timedelta(minutes=5)
        )
        assert result.rows_written == 1

        with engine.begin() as conn:
            row = conn.execute(
                text("SELECT available_at, availability_basis FROM fed_net_liquidity_daily WHERE obs_date = :d"),
                {"d": obs_date},
            ).mappings().fetchone()

        assert row["available_at"] == walcl_wtregen_pulled  # the LAST leg, not the first (RRP)
        assert row["available_at"] != rrp_pulled
        # Correctly close to release_at (~32 min after) -> observed_acquisition,
        # not the false "observed_acquisition ~27h early" the review flagged
        # when MIN let a stale available_at slip through the 1-day slack.
        assert row["availability_basis"] == "observed_acquisition"

    def test_available_at_is_never_earlier_than_release_at(self, godview_engine):
        """Even if every leg was (implausibly) pulled before the official
        release -- a leak -- available_at must be clamped to release_at, not
        report availability earlier than the release itself."""
        engine = godview_engine
        obs_date = date(2026, 9, 16)
        release_at, _ = compute_release_at(obs_date)
        leaked_pull = datetime.combine(obs_date, datetime.min.time(), tzinfo=timezone.utc) + timedelta(hours=10)
        assert leaked_pull < release_at

        with engine.begin() as conn:
            _insert_component(conn, WALCL_SERIES_ID, obs_date, 7_500_000.0, pull_timestamp=leaked_pull)
            _insert_component(conn, WTREGEN_SERIES_ID, obs_date, 700_000.0, pull_timestamp=leaked_pull)
            _insert_component(conn, RRP_SERIES_ID, obs_date, 300.0, pull_timestamp=leaked_pull)

        # as_of_ts after release, so the leaked pull now clears the
        # publication-lag PIT gate (that gate is about the caller's
        # reference time, not the row's stored available_at metadata).
        result = materialize_fed_liquidity(engine, code_sha=_CODE_SHA, as_of_ts=release_at + timedelta(minutes=1))
        assert result.rows_written == 1

        with engine.begin() as conn:
            row = conn.execute(
                text("SELECT available_at FROM fed_net_liquidity_daily WHERE obs_date = :d"), {"d": obs_date}
            ).mappings().fetchone()

        assert row["available_at"] == release_at  # clamped up to release_at, not the earlier leaked pull
        assert row["available_at"] > leaked_pull


class TestLedgerConsistency:
    """The ledger row and the returned MaterializationResult must agree on
    rows_skipped (an independent review of PR #677 @ bff082af found the
    ledger folding in `noop` counts that the returned result did not)."""

    def test_ledger_rows_skipped_matches_returned_result(self, godview_engine):
        engine = godview_engine
        noop_date = date(2026, 6, 3)
        written_date = date(2026, 6, 10)
        skipped_date = date(2026, 6, 17)  # missing RRP

        noop_release_at, _ = compute_release_at(noop_date)
        noop_after_release = noop_release_at + timedelta(hours=4)
        with engine.begin() as conn:
            _insert_component(conn, WALCL_SERIES_ID, noop_date, 7_000_000.0, pull_timestamp=noop_after_release)
            _insert_component(conn, WTREGEN_SERIES_ID, noop_date, 700_000.0, pull_timestamp=noop_after_release)
            _insert_component(conn, RRP_SERIES_ID, noop_date, 300.0, pull_timestamp=noop_after_release)
        # A prior run writes noop_date; the run under test below leaves it
        # unchanged (a genuine noop, not a skip).
        setup = materialize_fed_liquidity(
            engine, code_sha=_CODE_SHA, as_of_ts=noop_after_release + timedelta(minutes=5)
        )
        assert setup.rows_written == 1

        written_release_at, _ = compute_release_at(written_date)
        written_after_release = written_release_at + timedelta(hours=4)
        with engine.begin() as conn:
            _insert_component(conn, WALCL_SERIES_ID, written_date, 7_100_000.0, pull_timestamp=written_after_release)
            _insert_component(conn, WTREGEN_SERIES_ID, written_date, 710_000.0, pull_timestamp=written_after_release)
            _insert_component(conn, RRP_SERIES_ID, written_date, 310.0, pull_timestamp=written_after_release)
            # skipped_date: WALCL/WTREGEN present, RRP deliberately missing.
            _insert_component(conn, WALCL_SERIES_ID, skipped_date, 7_200_000.0, pull_timestamp=written_after_release)
            _insert_component(conn, WTREGEN_SERIES_ID, skipped_date, 720_000.0, pull_timestamp=written_after_release)

        result = materialize_fed_liquidity(
            engine, code_sha=_CODE_SHA, as_of_ts=written_after_release + timedelta(minutes=5)
        )

        assert any(r.obs_date == written_date and r.status == "written" for r in result.rows)
        assert any(r.obs_date == noop_date and r.status == "noop" for r in result.rows)
        assert any(r.obs_date == skipped_date and r.reason == "missing_rrp" for r in result.rows)

        with engine.begin() as conn:
            ledger = _run_ledger_row(conn, result.run_id)

        assert ledger is not None
        assert ledger["rows_skipped"] == result.rows_skipped
        # And the definition itself: only the genuine skip counts, not the noop.
        assert result.rows_skipped == 1


# ══════════════════════════════════════════════════════════════════════════
# Missing input -> no row (no fallback)
# ══════════════════════════════════════════════════════════════════════════


def test_missing_rrp_leaves_the_wednesday_unavailable_no_fallback(godview_engine):
    """WALCL and WTREGEN present, RRPONTSYD missing for that exact date -> no row at all."""
    engine = godview_engine
    obs_date = date(2026, 3, 4)  # a Wednesday
    release_at, _ = compute_release_at(obs_date)
    after_release = release_at + timedelta(hours=4)

    with engine.begin() as conn:
        _insert_component(conn, WALCL_SERIES_ID, obs_date, 7_400_000.0, pull_timestamp=after_release)
        _insert_component(conn, WTREGEN_SERIES_ID, obs_date, 650_000.0, pull_timestamp=after_release)
        # RRPONTSYD deliberately NOT inserted for this obs_date.

    result = materialize_fed_liquidity(engine, code_sha=_CODE_SHA, as_of_ts=after_release + timedelta(minutes=5))

    assert result.status == "SUCCESS_NOOP"
    assert result.rows_written == 0
    assert any(r.obs_date == obs_date and r.reason == SKIP_MISSING_RRP for r in result.rows)

    with engine.begin() as conn:
        assert _row(conn, obs_date) is None  # never a fabricated row


def test_missing_walcl_leaves_the_wednesday_unavailable_no_fallback(godview_engine):
    engine = godview_engine
    obs_date = date(2026, 3, 11)  # a Wednesday
    release_at, _ = compute_release_at(obs_date)
    after_release = release_at + timedelta(hours=4)

    with engine.begin() as conn:
        _insert_component(conn, WTREGEN_SERIES_ID, obs_date, 650_000.0, pull_timestamp=after_release)
        _insert_component(conn, RRP_SERIES_ID, obs_date, 280.0, pull_timestamp=after_release)
        # WALCL deliberately NOT inserted.

    result = materialize_fed_liquidity(engine, code_sha=_CODE_SHA, as_of_ts=after_release + timedelta(minutes=5))

    assert result.rows_written == 0
    assert any(r.obs_date == obs_date and r.reason == SKIP_MISSING_WALCL for r in result.rows)
    with engine.begin() as conn:
        assert _row(conn, obs_date) is None


def test_no_fred_history_at_all_is_empty_not_a_zero_row(godview_engine):
    engine = godview_engine
    result = materialize_fed_liquidity(engine, code_sha=_CODE_SHA)
    assert result.status == "EMPTY"
    assert result.rows_written == 0
    with engine.begin() as conn:
        ledger = _run_ledger_row(conn, result.run_id)
    assert ledger is not None
    assert ledger["status"] == "inputs_missing"


# ══════════════════════════════════════════════════════════════════════════
# Publication-lag PIT: a Wednesday value not usable before Thursday release
# (including the federal-holiday shift)
# ══════════════════════════════════════════════════════════════════════════


def test_a_wednesday_row_is_refused_before_its_thursday_release_even_if_leaked(godview_engine):
    """All three components exist AND were (implausibly) already pulled --
    but the caller's as_of_ts is before the H.4.1 release lag, so the writer
    still refuses to materialize the row."""
    engine = godview_engine
    obs_date = date(2026, 9, 16)  # a Wednesday
    release_at, _ = compute_release_at(obs_date)
    leaked_pull = datetime.combine(obs_date, datetime.min.time(), tzinfo=timezone.utc) + timedelta(hours=10)
    before_release = release_at - timedelta(hours=1)

    with engine.begin() as conn:
        _insert_component(conn, WALCL_SERIES_ID, obs_date, 7_500_000.0, pull_timestamp=leaked_pull)
        _insert_component(conn, WTREGEN_SERIES_ID, obs_date, 700_000.0, pull_timestamp=leaked_pull)
        _insert_component(conn, RRP_SERIES_ID, obs_date, 300.0, pull_timestamp=leaked_pull)

    result = materialize_fed_liquidity(engine, code_sha=_CODE_SHA, as_of_ts=before_release)

    assert result.rows_written == 0
    assert any(r.obs_date == obs_date and r.reason == SKIP_BEFORE_RELEASE for r in result.rows)
    with engine.begin() as conn:
        assert _row(conn, obs_date) is None

    after_release = release_at + timedelta(minutes=1)
    result2 = materialize_fed_liquidity(engine, code_sha=_CODE_SHA, as_of_ts=after_release)
    assert result2.status == "SUCCESS"
    assert result2.rows_written == 1


def test_thanksgiving_wednesday_is_refused_until_the_shifted_friday_release(godview_engine):
    """Thanksgiving 2026 (Thursday 2026-11-26) shifts the H.4.1 release for
    the preceding Wednesday to Friday 2026-11-27 -- refused before that."""
    engine = godview_engine
    obs_date = date(2026, 11, 25)
    release_at, note = compute_release_at(obs_date)
    assert release_at.date() == date(2026, 11, 27)

    pulled = datetime.combine(obs_date, datetime.min.time(), tzinfo=timezone.utc) + timedelta(hours=10)
    with engine.begin() as conn:
        _insert_component(conn, WALCL_SERIES_ID, obs_date, 7_500_000.0, pull_timestamp=pulled)
        _insert_component(conn, WTREGEN_SERIES_ID, obs_date, 700_000.0, pull_timestamp=pulled)
        _insert_component(conn, RRP_SERIES_ID, obs_date, 300.0, pull_timestamp=pulled)

    # obs_date (2026-11-25) is in the future relative to materialize_fed_liquidity's
    # default `as_of` (date.today()) whenever this test runs before that date --
    # read_window bounds obs_date <= as_of, so without an explicit as_of the
    # observations would not even be found (EMPTY), which is a different
    # failure mode than the PIT-gate refusal this test is pinning.
    naive_thursday_release = datetime.combine(date(2026, 11, 26), datetime.min.time(), tzinfo=timezone.utc) + timedelta(hours=21)
    result_thursday = materialize_fed_liquidity(
        engine, code_sha=_CODE_SHA, as_of=obs_date, as_of_ts=naive_thursday_release
    )
    assert result_thursday.rows_written == 0
    assert any(r.obs_date == obs_date and r.reason == SKIP_BEFORE_RELEASE for r in result_thursday.rows)

    result_friday = materialize_fed_liquidity(
        engine, code_sha=_CODE_SHA, as_of=obs_date, as_of_ts=release_at + timedelta(minutes=1)
    )
    assert result_friday.rows_written == 1


# ══════════════════════════════════════════════════════════════════════════
# Idempotent upsert: unchanged re-run is a no-op
# ══════════════════════════════════════════════════════════════════════════


def test_rerun_with_unchanged_inputs_is_a_noop(godview_engine):
    engine = godview_engine
    obs_date = date(2026, 6, 3)  # a Wednesday
    release_at, _ = compute_release_at(obs_date)
    after_release = release_at + timedelta(hours=4)

    with engine.begin() as conn:
        _insert_component(conn, WALCL_SERIES_ID, obs_date, 7_100_000.0, pull_timestamp=after_release)
        _insert_component(conn, WTREGEN_SERIES_ID, obs_date, 620_000.0, pull_timestamp=after_release)
        _insert_component(conn, RRP_SERIES_ID, obs_date, 250.0, pull_timestamp=after_release)

    as_of_ts = after_release + timedelta(minutes=5)
    first = materialize_fed_liquidity(engine, code_sha=_CODE_SHA, as_of_ts=as_of_ts)
    assert first.status == "SUCCESS"
    assert first.rows_written == 1

    second = materialize_fed_liquidity(engine, code_sha=_CODE_SHA, as_of_ts=as_of_ts)
    assert second.status == "SUCCESS_NOOP"
    assert second.rows_written == 0
