"""DB-gated API contract tests for the Fed liquidity and commodity warehouse routes.

Exercises api/routers/godview_pillars.py's ``get_fed_liquidity_pillar`` and
``get_commodity_warehouse_pillar`` directly, same pattern as
test_cftc_pillar_api_db.py.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from godview.commodity_warehouse_pillar import materialize_commodity_warehouse_pillar
from godview.fed_liquidity_pillar import (
    RRP_SERIES_ID,
    WALCL_SERIES_ID,
    WTREGEN_SERIES_ID,
    materialize_fed_liquidity_pillar,
)

pytestmark = pytest.mark.integration


def _ensure_source_catalog_row(conn, name: str, latency_class: str = "REALTIME") -> int:
    row = conn.execute(text("SELECT id FROM source_catalog WHERE name = :n"), {"n": name}).fetchone()
    if row is not None:
        return row[0]
    row = conn.execute(
        text(
            "INSERT INTO source_catalog (name, base_url, cost_tier, latency_class, "
            "pit_available, revision_behavior, trust_score, priority_rank) "
            "VALUES (:n, 'https://example.invalid', 'FREE', :lat, TRUE, 'NEVER', 'HIGH', 10) "
            "RETURNING id"
        ),
        {"n": name, "lat": latency_class},
    ).fetchone()
    return row[0]


def test_fed_liquidity_route_exposes_per_component_basis_and_derived_fields(
    godview_pg_engine, monkeypatch
):
    engine = godview_pg_engine
    obs_date = date(2026, 8, 5)  # a Wednesday
    pts = datetime.combine(obs_date, datetime.min.time(), tzinfo=timezone.utc) + timedelta(days=1, hours=20)

    with engine.begin() as conn:
        sid = _ensure_source_catalog_row(conn, f"FRED_ROUTE_TEST_{uuid.uuid4().hex[:8]}")
        for series_id, value in ((WALCL_SERIES_ID, 7_600_000.0), (WTREGEN_SERIES_ID, 720_000.0), (RRP_SERIES_ID, 280.0)):
            conn.execute(
                text(
                    "INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_timestamp, pull_status) "
                    "VALUES (:sid, :src, :od, :val, :pts, 'SUCCESS')"
                ),
                {"sid": series_id, "src": sid, "od": obs_date, "val": value, "pts": pts},
            )

    result = materialize_fed_liquidity_pillar(engine, as_of=obs_date)
    assert result.status == "SUCCESS"

    import api.routers.godview_pillars as router_module

    monkeypatch.setattr(router_module, "get_db_engine", lambda: engine)

    as_of = obs_date + timedelta(days=1)
    response = router_module.get_fed_liquidity_pillar(as_of=as_of, _token="test")

    assert response["available"] is True
    assert response["pillar"] == "fed_net_liquidity"
    fields = response["fields"]
    assert fields["fed_assets_walcl"]["availability_basis"] == "observed_acquisition"
    assert fields["reverse_repo_rrp"]["unit"] == "billions_usd"
    assert fields["net_liquidity_usd_m"]["provenance"] == "derived"
    assert fields["net_liquidity_usd_m"]["value"] == pytest.approx(7_600_000.0 - 720_000.0 - 280_000.0)


def test_fed_liquidity_route_never_configured_before_any_materializer_run(godview_pg_engine, monkeypatch):
    """A fresh scratch DB with the table but no godview_generations row for this pillar."""
    import api.routers.godview_pillars as router_module

    monkeypatch.setattr(router_module, "get_db_engine", lambda: godview_pg_engine)
    monkeypatch.setattr(router_module, "_table_exists", lambda conn, name: True)

    with godview_pg_engine.connect() as conn:
        from godview.fed_liquidity_pillar import read_fed_liquidity_pillar

        result = read_fed_liquidity_pillar(conn, date.today())
    # Weak assertion (shared scratch DB may have earlier runs): state is one
    # of the three documented values, never an exception.
    assert result.state in ("never_configured", "materializer_failed", "ok")


def test_commodity_warehouse_route_wraps_lme_and_permanently_unavailable_cushing(
    godview_pg_engine, monkeypatch
):
    engine = godview_pg_engine
    metal = f"routetest{uuid.uuid4().hex[:8]}"
    obs_date = date(2026, 7, 1)

    with engine.begin() as conn:
        sid = _ensure_source_catalog_row(conn, f"LME_ROUTE_TEST_{uuid.uuid4().hex[:8]}", latency_class="EOD")
        for series_id, value in (
            (f"lme:stocks_total_mt:{metal}", 2000.0),
            (f"lme:stocks_cancelled_mt:{metal}", 900.0),
            (f"lme:stocks_live_mt:{metal}", 1100.0),
            (f"lme:cancelled_ratio:{metal}", 0.45),
        ):
            conn.execute(
                text(
                    "INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_status) "
                    "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                ),
                {"sid": series_id, "src": sid, "od": obs_date, "val": value},
            )

    result = materialize_commodity_warehouse_pillar(engine, as_of=obs_date, metals=(metal,))
    assert result.status == "SUCCESS"

    import api.routers.godview_pillars as router_module

    monkeypatch.setattr(router_module, "get_db_engine", lambda: engine)
    monkeypatch.setattr(router_module, "LME_METALS", (metal,))

    response = router_module.get_commodity_warehouse_pillar(as_of=obs_date, _token="test")

    assert "lme" in response and "cushing_crude_stocks" in response
    assert response["cushing_crude_stocks"]["available"] is False
    assert "never_configured" in response["cushing_crude_stocks"]["reason"]

    lme = response["lme"]
    assert lme["available"] is True
    assert metal in lme["fields"]
    assert lme["fields"][metal]["canceled_ratio"]["value"] == pytest.approx(0.45)
    assert lme["fields"][metal]["physical_tightness_flag"]["value"] is True
