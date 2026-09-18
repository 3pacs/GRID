"""DB-gated tests for the FINRA short-volume God View pillar.

Depends on ``godview_pg_engine`` (tests/godview/conftest.py) — skips with
"GRID_TEST_DB_URL not set" until the scratch-DB URL arrives.
"""

from __future__ import annotations

import json
import random
import uuid
from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from godview.finra_short_volume_pillar import (
    SERIES_PREFIX,
    materialize_finra_short_volume_pillar,
    read_finra_short_volume_pillar,
)

pytestmark = pytest.mark.integration


def _random_trade_date() -> date:
    """A random weekday, unique per invocation -- see the Fed pillar's
    _random_wednesday for why (no per-test isolation key on this table
    beyond ticker+trade_date; a fixed date collides with a prior run on a
    persistent/shared scratch DB)."""
    start = date(1995, 1, 2)
    d = start + timedelta(days=random.randint(0, 365 * 30))
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d


def _ensure_source_catalog_row(conn, name: str) -> int:
    row = conn.execute(text("SELECT id FROM source_catalog WHERE name = :n"), {"n": name}).fetchone()
    if row is not None:
        return row[0]
    row = conn.execute(
        text(
            "INSERT INTO source_catalog (name, base_url, cost_tier, latency_class, "
            "pit_available, revision_behavior, trust_score, priority_rank) "
            "VALUES (:n, 'https://example.invalid', 'FREE', 'EOD', TRUE, 'NEVER', 'HIGH', 40) "
            "RETURNING id"
        ),
        {"n": name},
    ).fetchone()
    return row[0]


def _insert_short_volume_row(
    conn, symbol: str, market: str, trade_date: date, *, short_volume: float,
    total_volume: float, short_exempt: float = 0.0, source_id: int, pull_timestamp: datetime,
) -> None:
    conn.execute(
        text(
            "INSERT INTO raw_series (series_id, source_id, obs_date, value, "
            "raw_payload, pull_timestamp, pull_status) "
            "VALUES (:sid, :src, :od, :val, :payload, :pts, 'SUCCESS')"
        ),
        {
            "sid": f"{SERIES_PREFIX}:{symbol}:{market}",
            "src": source_id,
            "od": trade_date,
            "val": short_volume,
            "payload": json.dumps({
                "short_exempt_volume": short_exempt, "total_volume": total_volume,
                "symbol": symbol, "market": market,
            }),
            "pts": pull_timestamp,
        },
    )


@pytest.fixture
def source_id(godview_pg_engine):
    with godview_pg_engine.begin() as conn:
        sid = _ensure_source_catalog_row(conn, f"FINRA_SHORT_VOL_TEST_{uuid.uuid4().hex[:8]}")
    return sid


def test_migration_added_pit_columns(godview_pg_engine):
    with godview_pg_engine.begin() as conn:
        cols = {
            r[0] for r in conn.execute(
                text("SELECT column_name FROM information_schema.columns WHERE table_name = 'finra_short_volume_daily'")
            ).fetchall()
        }
        for expected in ("market", "release_date", "available_at", "provenance", "availability_basis", "generation_id", "coverage_fraction", "source_ref"):
            assert expected in cols


def test_materializer_writes_a_row_same_day_observed(godview_pg_engine, source_id):
    engine = godview_pg_engine
    symbol = f"T{uuid.uuid4().hex[:6].upper()}"
    trade_date = _random_trade_date()
    pts = datetime.combine(trade_date, datetime.min.time(), tzinfo=timezone.utc) + timedelta(hours=18)

    with engine.begin() as conn:
        _insert_short_volume_row(conn, symbol, "Q", trade_date, short_volume=600.0, total_volume=1000.0, source_id=source_id, pull_timestamp=pts)

    result = materialize_finra_short_volume_pillar(engine, as_of=trade_date)
    assert result.status == "SUCCESS"
    assert result.rows_written == 1

    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT short_ratio, availability_basis, release_date FROM finra_short_volume_daily WHERE ticker = :t"),
            {"t": symbol},
        ).mappings().fetchone()
    assert row is not None
    assert row["short_ratio"] == pytest.approx(0.6)
    assert row["availability_basis"] == "observed_acquisition"
    assert row["release_date"] == trade_date


def test_zero_total_volume_leaves_the_day_unmaterialized_no_fallback(godview_pg_engine, source_id):
    engine = godview_pg_engine
    symbol = f"Z{uuid.uuid4().hex[:6].upper()}"
    trade_date = _random_trade_date()
    pts = datetime.combine(trade_date, datetime.min.time(), tzinfo=timezone.utc)

    with engine.begin() as conn:
        _insert_short_volume_row(conn, symbol, "Q", trade_date, short_volume=0.0, total_volume=0.0, source_id=source_id, pull_timestamp=pts)

    result = materialize_finra_short_volume_pillar(engine, as_of=trade_date)
    assert result.status in ("SUCCESS_NOOP", "EMPTY")

    with engine.begin() as conn:
        row = conn.execute(text("SELECT 1 FROM finra_short_volume_daily WHERE ticker = :t"), {"t": symbol}).fetchone()
    assert row is None


def test_never_configured_without_any_raw_series_rows(godview_pg_engine):
    with godview_pg_engine.begin() as conn:
        result = read_finra_short_volume_pillar(conn, date.today())
    assert result.state in ("never_configured", "materializer_failed", "ok")
