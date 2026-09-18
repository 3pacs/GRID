"""DB-gated tests for the SEC FTD God View pillar.

Depends on ``godview_pg_engine`` (tests/godview/conftest.py) — skips with
"GRID_TEST_DB_URL not set" until the scratch-DB URL arrives.
"""

from __future__ import annotations

import json
import uuid
from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from godview.sec_ftd_pillar import (
    SERIES_PREFIX,
    materialize_sec_ftd_pillar,
    read_sec_ftd_pillar,
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
            "VALUES (:n, 'https://example.invalid', 'FREE', 'MONTHLY', TRUE, 'RARE', 'MED', 40) "
            "RETURNING id"
        ),
        {"n": name},
    ).fetchone()
    return row[0]


def _insert_ftd_row(
    conn, cusip: str, settlement_date: date, *, failed_shares: float, symbol: str | None,
    price: float | None, source_id: int, pull_timestamp: datetime,
) -> None:
    conn.execute(
        text(
            "INSERT INTO raw_series (series_id, source_id, obs_date, value, "
            "raw_payload, pull_timestamp, pull_status) "
            "VALUES (:sid, :src, :od, :val, :payload, :pts, 'SUCCESS')"
        ),
        {
            "sid": f"{SERIES_PREFIX}:{cusip}",
            "src": source_id,
            "od": settlement_date,
            "val": failed_shares,
            "payload": json.dumps({"symbol": symbol, "price": price, "cusip": cusip}),
            "pts": pull_timestamp,
        },
    )


@pytest.fixture
def source_id(godview_pg_engine):
    with godview_pg_engine.begin() as conn:
        sid = _ensure_source_catalog_row(conn, f"SEC_FTD_TEST_{uuid.uuid4().hex[:8]}")
    return sid


def test_migration_added_pit_columns(godview_pg_engine):
    with godview_pg_engine.begin() as conn:
        cols = {
            r[0] for r in conn.execute(
                text("SELECT column_name FROM information_schema.columns WHERE table_name = 'sec_regsho_ftd_cns'")
            ).fetchall()
        }
        for expected in ("release_date", "available_at", "provenance", "availability_basis", "generation_id", "coverage_fraction", "source_ref"):
            assert expected in cols


def test_materializer_writes_a_balance_row_with_symbol_and_dollar_value(godview_pg_engine, source_id):
    engine = godview_pg_engine
    cusip = f"Z{uuid.uuid4().hex[:8].upper()}"
    settlement_date = date(2026, 8, 17)  # second half of August -> release 2026-09-15
    release_date = date(2026, 9, 15)
    pts = datetime.combine(release_date, datetime.min.time(), tzinfo=timezone.utc) + timedelta(hours=6)

    with engine.begin() as conn:
        _insert_ftd_row(conn, cusip, settlement_date, failed_shares=373.0, symbol="HQ", price=16.99, source_id=source_id, pull_timestamp=pts)

    result = materialize_sec_ftd_pillar(engine, as_of=release_date)
    assert result.status == "SUCCESS"
    # NOT result.rows_written: raw_series is a shared, cumulative table
    # this materializer discovers EVERY sec:ftd_balance:* cusip from
    # (never truncated between test runs), so a global rows_written total
    # legitimately includes rows left by earlier tests/runs on a
    # persistent scratch DB (real-Postgres run, composition d92ca9fc: 4
    # rows across 4 CUSIPs, not 1). Scope to this test's own cusip + the
    # generation_id this call actually produced.
    with engine.begin() as conn:
        own_rows_written = conn.execute(
            text(
                "SELECT COUNT(*) FROM sec_regsho_ftd_cns WHERE cusip = :c AND generation_id = :gen"
            ),
            {"c": cusip, "gen": result.generation_id},
        ).scalar()
    assert own_rows_written == 1

    with engine.begin() as conn:
        row = conn.execute(
            text(
                "SELECT ticker, failed_shares, closing_price, total_failed_usd, "
                "mandatory_buyin_date, days_remaining, squeeze_risk_score, "
                "availability_basis, release_date "
                "FROM sec_regsho_ftd_cns WHERE cusip = :c"
            ),
            {"c": cusip},
        ).mappings().fetchone()
    assert row is not None
    assert row["ticker"] == "HQ"
    assert row["failed_shares"] == pytest.approx(373.0)
    assert row["total_failed_usd"] == pytest.approx(373.0 * 16.99)
    assert row["mandatory_buyin_date"] is None
    assert row["days_remaining"] is None
    assert row["squeeze_risk_score"] is None
    assert row["release_date"] == release_date
    assert row["availability_basis"] == "observed_acquisition"


def test_missing_symbol_falls_back_to_cusip(godview_pg_engine, source_id):
    engine = godview_pg_engine
    cusip = f"N{uuid.uuid4().hex[:8].upper()}"
    settlement_date = date(2026, 3, 3)  # first half -> release 2026-03-31
    release_date = date(2026, 3, 31)
    pts = datetime.combine(release_date, datetime.min.time(), tzinfo=timezone.utc)

    with engine.begin() as conn:
        _insert_ftd_row(conn, cusip, settlement_date, failed_shares=42.0, symbol=None, price=None, source_id=source_id, pull_timestamp=pts)

    result = materialize_sec_ftd_pillar(engine, as_of=release_date)
    assert result.status == "SUCCESS"

    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT ticker, closing_price, total_failed_usd, source_ref FROM sec_regsho_ftd_cns WHERE cusip = :c"),
            {"c": cusip},
        ).mappings().fetchone()
    assert row["ticker"] == cusip
    assert row["closing_price"] is None
    assert row["total_failed_usd"] is None
    assert "cusip_fallback" in row["source_ref"]


def test_never_configured_without_any_raw_series_rows(godview_pg_engine):
    with godview_pg_engine.begin() as conn:
        result = read_sec_ftd_pillar(conn, date.today())
    assert result.state in ("never_configured", "materializer_failed", "ok")
