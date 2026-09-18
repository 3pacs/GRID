"""DB-gated tests for the commodity warehouse God View pillar.

Depends on ``godview_pg_engine`` (tests/godview/conftest.py) — skips with
"GRID_TEST_DB_URL not set" until the scratch-DB URL arrives.
"""

from __future__ import annotations

import uuid
from datetime import date, timedelta

import pytest
from sqlalchemy import text

from godview.commodity_warehouse_pillar import (
    EXCHANGE,
    materialize_commodity_warehouse_pillar,
    read_commodity_warehouse_pillar,
)

pytestmark = pytest.mark.integration


def _ensure_source_catalog_row(conn, name: str) -> int:
    row = conn.execute(text("SELECT id FROM source_catalog WHERE name = :n"), {"n": name}).fetchone()
    if row is not None:
        return row[0]
    row = conn.execute(
        text(
            "INSERT INTO source_catalog (name, base_url, cost_tier, latency_class, "
            "pit_available, revision_behavior, trust_score, priority_rank) "
            "VALUES (:n, 'https://example.invalid', 'FREE', 'EOD', TRUE, 'NEVER', 'HIGH', 20) "
            "RETURNING id"
        ),
        {"n": name},
    ).fetchone()
    return row[0]


def _insert_lme_metal(conn, metal: str, obs_date: date, total: float, cancelled: float, *, source_id: int) -> None:
    live = total - cancelled
    ratio = cancelled / total if total > 0 else 0.0
    for series_id, value in (
        (f"lme:stocks_total_mt:{metal}", total),
        (f"lme:stocks_cancelled_mt:{metal}", cancelled),
        (f"lme:stocks_live_mt:{metal}", live),
        (f"lme:cancelled_ratio:{metal}", ratio),
    ):
        conn.execute(
            text(
                "INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_status) "
                "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
            ),
            {"sid": series_id, "src": source_id, "od": obs_date, "val": value},
        )


@pytest.fixture
def source_id(godview_pg_engine):
    with godview_pg_engine.begin() as conn:
        sid = _ensure_source_catalog_row(conn, f"LME_TEST_{uuid.uuid4().hex[:8]}")
    return sid


def test_migration_added_pit_columns(godview_pg_engine):
    with godview_pg_engine.begin() as conn:
        cols = {
            r[0]
            for r in conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'commodity_warehouse_inventories'"
                )
            ).fetchall()
        }
        for expected in (
            "release_date", "available_at", "provenance", "availability_basis",
            "generation_id", "coverage_fraction", "source_ref",
        ):
            assert expected in cols


def test_materializer_writes_a_row_reusing_the_pullers_own_ratio(godview_pg_engine, source_id):
    engine = godview_pg_engine
    metal = f"testmetal{uuid.uuid4().hex[:8]}"
    obs_date = date(2026, 9, 15)

    with engine.begin() as conn:
        _insert_lme_metal(conn, metal, obs_date, total=1000.0, cancelled=400.0, source_id=source_id)

    result = materialize_commodity_warehouse_pillar(engine, as_of=obs_date, metals=(metal,))
    assert result.status == "SUCCESS"
    assert result.rows_written == 1

    with engine.begin() as conn:
        row = conn.execute(
            text(
                "SELECT canceled_ratio, physical_tightness_flag, availability_basis, "
                "release_date, provenance FROM commodity_warehouse_inventories "
                "WHERE exchange = :exch AND commodity = :metal"
            ),
            {"exch": EXCHANGE, "metal": metal},
        ).mappings().fetchone()
    assert row is not None
    assert row["canceled_ratio"] == pytest.approx(0.4)  # reused from the puller, not recomputed
    assert row["physical_tightness_flag"] is True  # 0.4 >= 0.30 threshold
    assert row["availability_basis"] == "unknown"  # no cited LME schedule
    assert row["release_date"] is None
    assert row["provenance"] == "measured"


def test_missing_total_leaves_the_day_unmaterialized_no_fallback(godview_pg_engine, source_id):
    engine = godview_pg_engine
    metal = f"nototal{uuid.uuid4().hex[:8]}"
    obs_date = date(2026, 9, 15)

    with engine.begin() as conn:
        # Only the ratio series exists -- total is missing.
        conn.execute(
            text(
                "INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_status) "
                "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
            ),
            {"sid": f"lme:cancelled_ratio:{metal}", "src": source_id, "od": obs_date, "val": 0.5},
        )

    result = materialize_commodity_warehouse_pillar(engine, as_of=obs_date, metals=(metal,))
    assert result.status == "EMPTY" or result.rows_written == 0

    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT 1 FROM commodity_warehouse_inventories WHERE exchange = :exch AND commodity = :metal"),
            {"exch": EXCHANGE, "metal": metal},
        ).fetchone()
    assert row is None


def test_read_bounds_by_report_date_since_there_is_no_schedule_based_gate(godview_pg_engine, source_id):
    engine = godview_pg_engine
    metal = f"pit{uuid.uuid4().hex[:8]}"
    obs_date = date(2026, 6, 10)

    with engine.begin() as conn:
        _insert_lme_metal(conn, metal, obs_date, total=500.0, cancelled=50.0, source_id=source_id)

    result = materialize_commodity_warehouse_pillar(engine, as_of=obs_date, metals=(metal,))
    assert result.status == "SUCCESS"

    with engine.begin() as conn:
        before = read_commodity_warehouse_pillar(conn, obs_date - timedelta(days=1), metals=(metal,))
        on_day = read_commodity_warehouse_pillar(conn, obs_date, metals=(metal,))

    assert all(r["commodity"] != metal for r in before.rows)
    assert any(r["commodity"] == metal for r in on_day.rows)
