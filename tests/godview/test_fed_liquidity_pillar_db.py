"""DB-gated tests for the Fed net liquidity God View pillar.

Depends on ``godview_pg_engine`` (tests/godview/conftest.py) — skips with
"GRID_TEST_DB_URL not set" until the scratch-DB URL arrives.
"""

from __future__ import annotations

import random
import uuid
from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from godview.fed_liquidity_pillar import (
    RRP_SERIES_ID,
    WALCL_SERIES_ID,
    WTREGEN_SERIES_ID,
    materialize_fed_liquidity_pillar,
    read_fed_liquidity_pillar,
)

pytestmark = pytest.mark.integration


def _random_wednesday() -> date:
    """A Wednesday chosen fresh per call, from a ~40-year range.

    Root cause (2026-09-18, real-Postgres run, composition 42df4362):
    fed_net_liquidity_daily has no per-test isolation key analogous to
    cftc_positioning_daily's random contract_code or
    commodity_warehouse_inventories's random metal -- it is genuinely one
    global row per obs_date. Earlier versions of these tests used fixed
    dates (e.g. date(2026, 9, 16)); re-running the SAME test file against
    the SAME persistent/shared scratch DB a second time found those exact
    obs_dates already present from the first run and correctly reported
    materialize_fed_liquidity_pillar() -> SUCCESS_NOOP (idempotent no-op,
    not a bug -- see godview/fed_liquidity_pillar.py's own contract). That
    was misread as a regression from the unit-normalisation commit; the
    real fix is here, not in the materializer (confirmed with a fake
    reader in tests/godview/test_fed_liquidity_pillar_pure.py). A random
    Wednesday per test invocation makes a collision with any prior run
    astronomically unlikely, mirroring the uuid4-based isolation the other
    two pillars' tests already use.
    """
    start = date(1990, 1, 3)  # a Wednesday
    return start + timedelta(weeks=random.randint(0, 52 * 40))


def _ensure_source_catalog_row(conn, name: str) -> int:
    row = conn.execute(text("SELECT id FROM source_catalog WHERE name = :n"), {"n": name}).fetchone()
    if row is not None:
        return row[0]
    row = conn.execute(
        text(
            "INSERT INTO source_catalog (name, base_url, cost_tier, latency_class, "
            "pit_available, revision_behavior, trust_score, priority_rank) "
            "VALUES (:n, 'https://example.invalid', 'FREE', 'REALTIME', TRUE, 'RARE', 'HIGH', 10) "
            "RETURNING id"
        ),
        {"n": name},
    ).fetchone()
    return row[0]


def _insert_component(conn, series_id: str, obs_date: date, value: float, *, source_id: int, pull_timestamp: datetime | None = None) -> None:
    conn.execute(
        text(
            "INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_timestamp, pull_status) "
            "VALUES (:sid, :src, :od, :val, COALESCE(:pts, NOW()), 'SUCCESS')"
        ),
        {"sid": series_id, "src": source_id, "od": obs_date, "val": value, "pts": pull_timestamp},
    )


@pytest.fixture
def source_id(godview_pg_engine):
    with godview_pg_engine.begin() as conn:
        sid = _ensure_source_catalog_row(conn, f"FRED_TEST_{uuid.uuid4().hex[:8]}")
    return sid


def test_migration_added_pit_columns(godview_pg_engine):
    with godview_pg_engine.begin() as conn:
        cols = {
            r[0]
            for r in conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'fed_net_liquidity_daily'"
                )
            ).fetchall()
        }
        for expected in (
            "release_date", "available_at", "walcl_pulled_at", "wtregen_pulled_at",
            "rrp_pulled_at", "provenance", "availability_basis", "generation_id",
            "coverage_fraction", "source_ref",
        ):
            assert expected in cols


def test_materializer_writes_a_row_on_a_wednesday_when_all_three_components_present(
    godview_pg_engine, source_id
):
    engine = godview_pg_engine
    obs_date = _random_wednesday()
    on_schedule = datetime.combine(obs_date, datetime.min.time(), tzinfo=timezone.utc) + timedelta(days=1, hours=20)

    with engine.begin() as conn:
        _insert_component(conn, WALCL_SERIES_ID, obs_date, 7_500_000.0, source_id=source_id, pull_timestamp=on_schedule)
        _insert_component(conn, WTREGEN_SERIES_ID, obs_date, 700_000.0, source_id=source_id, pull_timestamp=on_schedule)
        _insert_component(conn, RRP_SERIES_ID, obs_date, 300.0, source_id=source_id, pull_timestamp=on_schedule)

    result = materialize_fed_liquidity_pillar(engine, as_of=obs_date)
    assert result.status == "SUCCESS"
    assert result.rows_written == 1

    with engine.begin() as conn:
        row = conn.execute(
            text(
                "SELECT net_liquidity_usd_m, availability_basis, release_date, liquidity_regime "
                "FROM fed_net_liquidity_daily WHERE obs_date = :d"
            ),
            {"d": obs_date},
        ).mappings().fetchone()
    assert row is not None
    assert row["net_liquidity_usd_m"] == pytest.approx(7_500_000.0 - 700_000.0 - 300_000.0)
    assert row["availability_basis"] == "observed_acquisition"
    assert row["release_date"] == obs_date + timedelta(days=1)
    assert row["liquidity_regime"] == "insufficient_history"  # no 30d-prior history yet


def test_missing_component_leaves_the_wednesday_unavailable_no_fallback(
    godview_pg_engine, source_id
):
    """WALCL and WTREGEN present, RRPONTSYD missing for that exact date -> no row at all."""
    engine = godview_pg_engine
    obs_date = _random_wednesday()
    pts = datetime.combine(obs_date, datetime.min.time(), tzinfo=timezone.utc) + timedelta(days=1)

    with engine.begin() as conn:
        _insert_component(conn, WALCL_SERIES_ID, obs_date, 7_400_000.0, source_id=source_id, pull_timestamp=pts)
        _insert_component(conn, WTREGEN_SERIES_ID, obs_date, 650_000.0, source_id=source_id, pull_timestamp=pts)
        # RRPONTSYD deliberately NOT inserted for this obs_date.

    result = materialize_fed_liquidity_pillar(engine, as_of=obs_date)
    assert result.status == "SUCCESS_NOOP"
    assert result.rows_written == 0

    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT 1 FROM fed_net_liquidity_daily WHERE obs_date = :d"), {"d": obs_date}
        ).fetchone()
    assert row is None


def test_pit_read_excludes_a_row_released_after_as_of(godview_pg_engine, source_id):
    engine = godview_pg_engine
    obs_date = _random_wednesday()
    pts = datetime.combine(obs_date, datetime.min.time(), tzinfo=timezone.utc) + timedelta(days=1, hours=20)

    with engine.begin() as conn:
        _insert_component(conn, WALCL_SERIES_ID, obs_date, 7_450_000.0, source_id=source_id, pull_timestamp=pts)
        _insert_component(conn, WTREGEN_SERIES_ID, obs_date, 680_000.0, source_id=source_id, pull_timestamp=pts)
        _insert_component(conn, RRP_SERIES_ID, obs_date, 250.0, source_id=source_id, pull_timestamp=pts)

    result = materialize_fed_liquidity_pillar(engine, as_of=obs_date)
    assert result.status == "SUCCESS"

    release_date = obs_date + timedelta(days=1)
    with engine.begin() as conn:
        before = read_fed_liquidity_pillar(conn, release_date - timedelta(days=1))
        after = read_fed_liquidity_pillar(conn, release_date)

    assert all(r["obs_date"] != obs_date for r in before.rows)
    assert any(r["obs_date"] == obs_date for r in after.rows)
