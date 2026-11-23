"""``_build_risk_map`` reads its exact series through real SQL, past decoys.

The risk map once read its inputs with ``fr.name ILIKE :pattern ORDER BY
rs.obs_date DESC LIMIT 1``. On griddb (2026-11-23) each pattern matched
several features. HY came from whichever of hy_oas_spread,
hy_spread_3m_chg, hy_spread_proxy and hy_spread was newest. The TGA was
tga_drawdown (a 30-day change) or mortgage_30y. TED was a 2022 reading of a
discontinued series. VIX matched nothing.

This seeds the real inputs alongside the decoys from griddb, each decoy
dated *newer* than the real series. It adds multi-vintage dates and a
retracted vintage, then runs the builder on a scratch schema through the
real ``store.pit.PITStore`` SQL. The decoys must never be read. Each date
counts once, at its newest unretracted vintage. HY / IG are in basis points.

Runs on ``pg_engine`` (``GRID_TEST_DB_URL``) and skips without PostgreSQL.
"""

from __future__ import annotations

import sys
import types
from datetime import date, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

pytestmark = pytest.mark.xdist_group("postgres")

_DDL = (
    "CREATE TABLE feature_registry (id SERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE)",
    """
    CREATE TABLE resolved_series (
        id                    BIGSERIAL PRIMARY KEY,
        feature_id            INTEGER NOT NULL REFERENCES feature_registry(id),
        obs_date              DATE NOT NULL,
        release_date          DATE NOT NULL,
        vintage_date          DATE NOT NULL,
        value                 DOUBLE PRECISION NOT NULL,
        source_priority_used  INTEGER NOT NULL DEFAULT 1
    )
    """,
    "CREATE UNIQUE INDEX uq_resolved_series_composite ON resolved_series (feature_id, obs_date, vintage_date)",
    """
    CREATE TABLE resolved_series_retractions (
        id            BIGSERIAL PRIMARY KEY,
        feature_id    INTEGER NOT NULL,
        obs_date      DATE NOT NULL,
        vintage_date  DATE NOT NULL,
        retracted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        reason        TEXT NOT NULL,
        run_tag       TEXT NOT NULL,
        UNIQUE (feature_id, obs_date, vintage_date)
    )
    """,
)

TODAY = date.today()


def _ago(days: int) -> date:
    return TODAY - timedelta(days=days)


# (obs_date, vintage_date, value). release_date = vintage_date.
REAL = {
    # 1y of weekly VIX closes. Two vintages on _ago(7) (newest wins), and on
    # _ago(14) a newer vintage of 99.0 that is retracted.
    "vix_spot": [(_ago(7 * k), _ago(7 * k), v) for k, v in
                 ((49, 12.0), (42, 14.0), (35, 15.0), (28, 16.0), (21, 18.0), (3, 22.0))]
    + [(_ago(14), _ago(14), 19.0), (_ago(14), _ago(13), 99.0),
       (_ago(7), _ago(7), 40.0), (_ago(7), _ago(6), 20.0),
       (_ago(0), _ago(0), 30.0)],
    "hy_oas_spread": [(_ago(33), _ago(30), 3.38), (_ago(3), _ago(0), 3.76)],
    "ig_oas_spread": [(_ago(33), _ago(30), 0.91), (_ago(3), _ago(0), 0.98)],
    "fed_balance_sheet": [(_ago(33), _ago(32), 6_753_604.0), (_ago(5), _ago(4), 6_741_452.0)],
    "treasury_general_acct": [(_ago(33), _ago(32), 952_002.0), (_ago(5), _ago(4), 916_613.0)],
    "reverse_repo_usage": [(_ago(31), _ago(31), 0.20), (_ago(0), _ago(0), 0.35)],
}
# Same-pattern features from griddb, each newer than the real series.
DECOYS = {
    "hy_spread_3m_chg": [(_ago(40), _ago(40), 0.10), (_ago(0), _ago(0), 0.83)],
    "hy_spread_proxy": [(_ago(1), _ago(1), 3.52)],
    "hy_spread": [(_ago(0), _ago(0), 9.99)],
    "ig_spread_proxy": [(_ago(0), _ago(0), 5.55)],
    "ted_spread": [(date(2022, 1, 21), date(2022, 1, 21), 0.09)],
    "tga_drawdown": [(_ago(33), _ago(33), 74_974.0), (_ago(0), _ago(0), 33_240.0)],
    "treasury_tga_balance": [(_ago(236), _ago(235), 847_718.0)],
    "mortgage_30y": [(_ago(0), _ago(0), 6.0)],
    "reverse_repo": [(_ago(0), _ago(0), 777.0)],
    "vix": [(_ago(0), _ago(0), 11.0)],
    "vix_close_decoy": [(_ago(0), _ago(0), 88.0)],
}


@pytest.fixture()
def scratch(pg_engine: Engine):
    schema = f"risk_map_series_{uuid4().hex[:12]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(pg_engine.url, connect_args={"options": f"-csearch_path={schema} -ctimezone=UTC"})
    try:
        with engine.begin() as conn:
            for ddl in _DDL:
                conn.execute(text(ddl))
            for name, rows in {**REAL, **DECOYS}.items():
                fid = conn.execute(
                    text("INSERT INTO feature_registry (name) VALUES (:n) RETURNING id"), {"n": name}
                ).scalar_one()
                conn.execute(
                    text("INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value) "
                         "VALUES (:f, :od, :vd, :vd, :v)"),
                    [{"f": fid, "od": od, "vd": vd, "v": v} for od, vd, v in rows],
                )
            conn.execute(
                text("INSERT INTO resolved_series_retractions (feature_id, obs_date, vintage_date, "
                     "retracted_at, reason, run_tag) "
                     "SELECT id, :od, :vd, NOW() - INTERVAL '2 days', 'bad print', 'test' "
                     "FROM feature_registry WHERE name = 'vix_spot'"),
                {"od": _ago(14), "vd": _ago(13)},
            )
        yield engine
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


@pytest.fixture()
def risk_map(scratch, monkeypatch):
    from api.routers import intelligence_risk as ir

    mod = types.ModuleType("physics.dealer_gamma")

    class DealerGammaEngine:  # test stub
        def __init__(self, engine):
            pass

        def compute_gex_profile(self, ticker):
            return {"error": "no options chain in the scratch schema"}

    mod.DealerGammaEngine = DealerGammaEngine
    monkeypatch.setitem(sys.modules, "physics.dealer_gamma", mod)
    monkeypatch.setattr(ir, "get_db_engine", lambda: scratch)
    ir._risk_map_cache.clear()
    try:
        yield ir._build_risk_map()
    finally:
        ir._risk_map_cache.clear()


def test_volatility_reads_vix_spot_one_value_per_date(risk_map):
    vol = risk_map["volatility_risk"]
    assert vol["available"] is True, vol
    assert vol["vix"] == 30.0
    assert vol["vix_as_of"] == TODAY.isoformat()
    # 9 dates, newest unretracted vintage each: 12 14 15 16 18 19 20 22 30.
    # The superseded 40.0 and the retracted 99.0 are not in the sample.
    assert vol["vix_percentile_sample"] == 9
    assert vol["vix_percentile_1y"] == 100


def test_credit_reads_oas_in_basis_points_not_the_decoys(risk_map):
    credit = risk_map["credit_risk"]
    assert credit["available"] is True, credit
    assert credit["hy_spread"] == 376
    assert credit["hy_spread_as_of"] == _ago(3).isoformat()
    assert credit["ig_spread"] == 98
    assert credit["spread_direction"] == "widening"
    assert credit["risk_level"] == "elevated"
    assert "ted_spread" not in credit


def test_liquidity_reads_walcl_wtregen_rrpontsyd_not_the_decoys(risk_map):
    liq = risk_map["liquidity_risk"]
    assert liq["available"] is True, liq
    assert liq["fed_net_liquidity_change_1m"] == -12_152
    assert liq["fed_balance_sheet_as_of"] == _ago(5).isoformat()
    # tga_drawdown (74,974 -> 33,240) would read "draining".
    assert liq["tga_trend"] == "stable"
    assert liq["reverse_repo_trend"] == "rising"


def test_only_series_subsystems_are_measured(risk_map):
    assert risk_map["available_subsystems"] == 3
    assert sorted(risk_map["unavailable_subsystems"]) == ["Concentration", "Correlation", "Dealer positioning"]
