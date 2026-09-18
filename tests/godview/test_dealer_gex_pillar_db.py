"""DB-gated tests for the dealer GEX God View pillar.

Depends on ``godview_pg_engine`` (tests/godview/conftest.py) — skips with
"GRID_TEST_DB_URL not set" until the scratch-DB URL arrives.
"""

from __future__ import annotations

import uuid
from datetime import date, timedelta

import pytest
from sqlalchemy import text

from godview.dealer_gex_pillar import (
    materialize_dealer_gex_pillar,
    read_dealer_gex_pillar,
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
            "VALUES (:n, 'https://example.invalid', 'FREE', 'EOD', TRUE, 'NEVER', 'HIGH', 50) "
            "RETURNING id"
        ),
        {"n": name},
    ).fetchone()
    return row[0]


def _seed_spot_price(conn, ticker: str, spot: float, as_of: date, *, source_id: int) -> None:
    """A feature_registry/resolved_series row that _resolve_feature_names + PITStore
    can find: candidate name f"{ticker.lower()}_close" (one of the fallback
    conventions api/routers/watchlist_helpers.py::_resolve_feature_names tries)."""
    feature_name = f"{ticker.lower()}_close"
    row = conn.execute(text("SELECT id FROM feature_registry WHERE name = :n"), {"n": feature_name}).fetchone()
    if row is None:
        row = conn.execute(
            text(
                "INSERT INTO feature_registry (name, family, description, transformation, "
                "normalization, missing_data_policy, eligible_from_date) "
                "VALUES (:n, 'equity', 'test spot price', 'raw', 'RAW', 'NAN', :d) RETURNING id"
            ),
            {"n": feature_name, "d": as_of - timedelta(days=365)},
        ).fetchone()
    feature_id = row[0]
    conn.execute(
        text(
            "INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, "
            "value, source_priority_used) VALUES (:fid, :od, :rd, :vd, :val, :src) "
            "ON CONFLICT (feature_id, obs_date, vintage_date) DO NOTHING"
        ),
        {"fid": feature_id, "od": as_of, "rd": as_of, "vd": as_of, "val": spot, "src": source_id},
    )


def _insert_snapshot_row(conn, ticker: str, snap_date: date, expiry: date, opt_type: str, strike: float, *, oi: float, iv: float | None) -> None:
    conn.execute(
        text(
            "INSERT INTO options_snapshots (ticker, snap_date, expiry, opt_type, strike, "
            "open_interest, implied_vol) VALUES (:t, :sd, :ex, :ot, :k, :oi, :iv) "
            "ON CONFLICT (ticker, snap_date, expiry, opt_type, strike) DO NOTHING"
        ),
        {"t": ticker, "sd": snap_date, "ex": expiry, "ot": opt_type, "k": strike, "oi": oi, "iv": iv},
    )


@pytest.fixture
def source_id(godview_pg_engine):
    with godview_pg_engine.begin() as conn:
        sid = _ensure_source_catalog_row(conn, f"GEX_TEST_{uuid.uuid4().hex[:8]}")
    return sid


def test_migration_added_pit_columns(godview_pg_engine):
    with godview_pg_engine.begin() as conn:
        cols = {
            r[0] for r in conn.execute(
                text("SELECT column_name FROM information_schema.columns WHERE table_name = 'dealer_gex_daily'")
            ).fetchall()
        }
        for expected in ("release_date", "available_at", "provenance", "availability_basis", "generation_id", "coverage_fraction", "contracts_used", "contracts_present", "source_ref"):
            assert expected in cols


def test_materializer_computes_gex_with_a_resolved_spot(godview_pg_engine, source_id):
    engine = godview_pg_engine
    ticker = f"T{uuid.uuid4().hex[:8].upper()}"
    snap_date = date(2026, 1, 5)
    expiry = date(2026, 4, 5)
    spot = 100.0

    with engine.begin() as conn:
        _seed_spot_price(conn, ticker, spot, snap_date, source_id=source_id)
        _insert_snapshot_row(conn, ticker, snap_date, expiry, "put", 99.0, oi=100.0, iv=0.3)
        _insert_snapshot_row(conn, ticker, snap_date, expiry, "call", 101.0, oi=100.0, iv=0.3)

    result = materialize_dealer_gex_pillar(engine, as_of=snap_date)
    assert result.status == "SUCCESS"
    assert result.rows_written == 1

    with engine.begin() as conn:
        row = conn.execute(
            text(
                "SELECT spot_price, gamma_flip_strike, provenance, availability_basis, "
                "contracts_used, contracts_present FROM dealer_gex_daily WHERE ticker = :t"
            ),
            {"t": ticker},
        ).mappings().fetchone()
    assert row is not None
    assert row["spot_price"] == pytest.approx(spot)
    assert row["gamma_flip_strike"] is not None
    assert 99.0 <= row["gamma_flip_strike"] <= 101.0
    assert row["provenance"] == "modeled"
    assert row["contracts_used"] == 2
    assert row["contracts_present"] == 2


def test_missing_spot_price_leaves_the_ticker_unmaterialized_no_fallback(godview_pg_engine):
    engine = godview_pg_engine
    ticker = f"N{uuid.uuid4().hex[:8].upper()}"
    snap_date = date(2026, 2, 3)
    expiry = date(2026, 5, 3)

    with engine.begin() as conn:
        # No feature_registry/resolved_series row for this ticker at all.
        _insert_snapshot_row(conn, ticker, snap_date, expiry, "call", 100.0, oi=100.0, iv=0.3)

    result = materialize_dealer_gex_pillar(engine, as_of=snap_date)
    assert result.status in ("SUCCESS_NOOP", "EMPTY")

    with engine.begin() as conn:
        row = conn.execute(text("SELECT 1 FROM dealer_gex_daily WHERE ticker = :t"), {"t": ticker}).fetchone()
    assert row is None


def test_never_configured_without_any_options_snapshots(godview_pg_engine):
    with godview_pg_engine.begin() as conn:
        result = read_dealer_gex_pillar(conn, date.today())
    assert result.state in ("never_configured", "materializer_failed", "ok")
